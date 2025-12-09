window.renderSentimentChart = function(data){
    const labels = data.map(d => d.classification);
    const values = data.map(d => d.count);
    const ctx = document.getElementById('sentimentChart').getContext('2d');
    new Chart(ctx, {
        type: 'pie',
        data: { labels, datasets: [{ data: values, backgroundColor: ['#2ecc71','#e74c3c','#95a5a6'] }] }
    });
}

window.renderTopProducts = function(data){
    const labels = data.map(d => d.productName);
    const values = data.map(d => d.totalComments);
    const ctx = document.getElementById('topProducts').getContext('2d');
    new Chart(ctx, {
        type: 'bar',
        data: { labels, datasets: [{ label: 'Comentarios', data: values, backgroundColor: '#3498db' }] },
        options: { indexAxis: 'y' }
    });
}

window.renderTrend = function(data){
    const labels = data.map(d => d.period);
    const values = data.map(d => d.avgSatisfaction || 0);
    const ctx = document.getElementById('trendChart').getContext('2d');
    new Chart(ctx, {
        type: 'line',
        data: { labels, datasets: [{ label: 'Satisfacción promedio', data: values, borderColor: '#9b59b6', tension: 0.3 }] }
    });
}
