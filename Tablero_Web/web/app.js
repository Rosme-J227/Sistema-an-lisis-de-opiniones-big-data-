async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

function formatPercent(v) {
  return (v * 100).toFixed(1) + '%';
}

let charts = {};

async function draw() {
  try {
    // limpiar cualquier error previo
    showError(null);
    const topNSelect = document.getElementById('topN');
    const topN = parseInt(topNSelect.value, 10);

    const total = await fetchJson('/api/total-comments');
    document.getElementById('total-comments').innerText = total.TotalComentariosProcesados.toLocaleString();

    const byProductDate = await fetchJson('/api/opinions-by-product-date');
    const definition = document.getElementById('satisfactionDefinition').value || 'rating';
    const pctByProduct = await fetchJson(`/api/pct-satisfied-by-product?definition=${definition}`);
    const trend = await fetchJson(`/api/satisfaction-trend?definition=${definition}`);

    // Prepare dates and top products
    const datesSet = new Set();
    const productTotals = {};
    byProductDate.forEach(r => {
      datesSet.add(r.Fecha);
      productTotals[r.ProductoNombre] = (productTotals[r.ProductoNombre] || 0) + r.TotalOpiniones;
    });
    const dates = Array.from(datesSet).sort();

    const topProducts = Object.entries(productTotals)
      .sort((a,b)=> b[1]-a[1])
      .slice(0, topN)
      .map(x=>x[0]);

    // Build datasets per product
    const datasets = topProducts.map((prod, idx) => {
      const dataByDate = dates.map(d => {
        const row = byProductDate.find(r => r.Fecha === d && r.ProductoNombre === prod);
        return row ? row.TotalOpiniones : 0;
      });
      const color = `hsl(${(idx*45)%360} 70% 45%)`;
      return {
        label: prod,
        data: dataByDate,
        backgroundColor: color,
        borderColor: color,
        borderWidth: 1
      };
    });

    // Opinions by product & date (stacked)
    const ctx1 = document.getElementById('opinionsByProductDate').getContext('2d');
    if (charts.opinionsByProductDate) charts.opinionsByProductDate.destroy();
    charts.opinionsByProductDate = new Chart(ctx1, {
      type: 'bar',
      data: { labels: dates, datasets },
      options: {
        plugins: { tooltip: { mode: 'index' } },
        responsive: true,
        scales: { x: { stacked: true }, y: { stacked: true, beginAtZero: true } }
      }
    });

    // Total opinions by product (bars)
    const prodLabels = pctByProduct.map(r=> r.ProductoNombre);
    const prodTotals = pctByProduct.map(r=> r.TotalOpiniones);
    const ctx4 = document.getElementById('totalOpinionsByProduct').getContext('2d');
    if (charts.totalOpinionsByProduct) charts.totalOpinionsByProduct.destroy();
    charts.totalOpinionsByProduct = new Chart(ctx4, {
      type: 'bar',
      data: { labels: prodLabels, datasets: [{ label: 'Opiniones', data: prodTotals, backgroundColor: prodLabels.map((_,i)=>`hsl(${(i*40)%360} 65% 50%)`)}] },
      options: { responsive: true, scales: { y: { beginAtZero: true } } }
    });

    // Pct satisfied by product (horizontal bars)
    const pctLabels = pctByProduct.map(r=> r.ProductoNombre);
    const pctValues = pctByProduct.map(r=> parseFloat(r.PctSatisfechos));
    const ctx2 = document.getElementById('pctSatisfiedByProduct').getContext('2d');
    if (charts.pctSatisfiedByProduct) charts.pctSatisfiedByProduct.destroy();
    charts.pctSatisfiedByProduct = new Chart(ctx2, {
      type: 'bar',
      data: { labels: pctLabels, datasets: [{ label: 'Pct Satisfechos', data: pctValues, backgroundColor: pctLabels.map((_,i)=>`hsl(${(i*55)%360} 60% 45%)`) }] },
      options: {
        indexAxis: 'y',
        responsive: true,
        plugins: { tooltip: { callbacks: { label: (ctx)=> formatPercent(ctx.raw) } } },
        scales: { x: { ticks: { callback: v => (v*100).toFixed(0)+'%' } } }
      }
    });

    // Satisfaction trend
    const trendDates = trend.map(r=> r.Fecha);
    const trendVals = trend.map(r=> parseFloat(r.PctSatisfechos));
    const ctx3 = document.getElementById('satisfactionTrend').getContext('2d');
    if (charts.satisfactionTrend) charts.satisfactionTrend.destroy();
    charts.satisfactionTrend = new Chart(ctx3, {
      type: 'line',
      data: { labels: trendDates, datasets: [{ label: 'Pct Satisfechos', data: trendVals, borderColor: 'rgb(33,120,220)', tension: 0.2, fill: false }] },
      options: { responsive: true, plugins: { tooltip: { callbacks: { label: (ctx)=> formatPercent(ctx.raw) } } }, scales: { y: { ticks: { callback: v => (v*100).toFixed(0)+'%' } } } }
    });

  } catch (err) {
    console.error('Error al dibujar dashboard', err);
    showError(err.message || String(err));
    // destruir charts existentes para evitar visuales inconsistentes
    Object.values(charts).forEach(c => { try { c.destroy(); } catch(e){} });
  }
}

function showError(message) {
  const el = document.getElementById('error');
  if (!el) return;
  if (!message) {
    el.classList.add('d-none');
    el.innerText = '';
  } else {
    el.classList.remove('d-none');
    el.innerText = message;
  }
}

  document.getElementById('topN').addEventListener('change', () => draw());
  document.getElementById('satisfactionDefinition').addEventListener('change', () => draw());

draw();
