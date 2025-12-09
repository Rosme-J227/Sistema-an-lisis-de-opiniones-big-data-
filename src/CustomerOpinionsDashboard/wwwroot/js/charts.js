


const charts = {
    sentiment: null,
    topProducts: null,
    trend: null,
    channel: null
};


const colors = {
    positive: '#198754',
    negative: '#dc3545',
    neutral: '#0dcaf0',
    primary: '#0d6efd',
    success: '#198754',
    danger: '#dc3545',
    info: '#0dcaf0',
    warning: '#ffc107',
    secondary: '#6c757d',
    light: '#f8f9fa'
};


function initSentimentChart(data) {
    const ctx = document.getElementById('sentimentChart');
    if (!ctx) return;


    const labels = data.map(d => d.classification || 'Unknown');
    const values = data.map(d => d.count || 0);
    const chartColors = data.map(d => {
        switch (d.classification?.toLowerCase()) {
            case 'positive':
                return colors.positive;
            case 'negative':
                return colors.negative;
            case 'neutral':
                return colors.neutral;
            default:
                return colors.secondary;
        }
    });


    if (charts.sentiment) {
        charts.sentiment.destroy();
    }

    charts.sentiment = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: labels,
            datasets: [{
                data: values,
                backgroundColor: chartColors,
                borderColor: '#fff',
                borderWidth: 2,
                borderRadius: 4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: {
                        padding: 15,
                        font: { size: 12 },
                        usePointStyle: true
                    }
                },
                tooltip: {
                    backgroundColor: 'rgba(0, 0, 0, 0.8)',
                    padding: 12,
                    titleFont: { size: 13 },
                    bodyFont: { size: 12 },
                    callbacks: {
                        label: function (context) {
                            const total = context.dataset.data.reduce((a, b) => a + b, 0);
                            const percentage = ((context.parsed / total) * 100).toFixed(1);
                            return `${context.label}: ${context.parsed} (${percentage}%)`;
                        }
                    }
                }
            }
        }
    });
}


function initTopProductsChart(data) {
    const ctx = document.getElementById('topProductsChart');
    if (!ctx) return;


    const labels = data.slice(0, 8).map(d => d.productName || 'Unknown');
    const comments = data.slice(0, 8).map(d => d.totalComments || 0);
    const satisfaction = data.slice(0, 8).map(d => d.avgSatisfaction || 0);


    if (charts.topProducts) {
        charts.topProducts.destroy();
    }

    charts.topProducts = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Total Comments',
                    data: comments,
                    backgroundColor: colors.primary,
                    borderRadius: 4,
                    borderSkipped: false,
                    yAxisID: 'y'
                },
                {
                    label: 'Avg Satisfaction',
                    data: satisfaction,
                    backgroundColor: colors.success,
                    borderRadius: 4,
                    borderSkipped: false,
                    yAxisID: 'y1'
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: {
                    display: true,
                    position: 'top',
                    labels: {
                        padding: 15,
                        font: { size: 12 }
                    }
                },
                tooltip: {
                    backgroundColor: 'rgba(0, 0, 0, 0.8)',
                    padding: 12,
                    titleFont: { size: 13 },
                    bodyFont: { size: 12 }
                }
            },
            scales: {
                x: {
                    stacked: false,
                    ticks: {
                        font: { size: 11 },
                        maxRotation: 45,
                        minRotation: 0
                    }
                },
                y: {
                    type: 'linear',
                    display: true,
                    position: 'left',
                    title: {
                        display: true,
                        text: 'Comments',
                        font: { size: 11, weight: 'bold' }
                    },
                    ticks: {
                        beginAtZero: true,
                        font: { size: 10 }
                    }
                },
                y1: {
                    type: 'linear',
                    display: true,
                    position: 'right',
                    title: {
                        display: true,
                        text: 'Satisfaction',
                        font: { size: 11, weight: 'bold' }
                    },
                    ticks: {
                        beginAtZero: true,
                        max: 100,
                        font: { size: 10 }
                    },
                    grid: { drawOnChartArea: false }
                }
            }
        }
    });
}


function initTrendChart(data) {
    const ctx = document.getElementById('trendChart');
    if (!ctx) return;


    const labels = data.map(d => d.period || 'Unknown');
    const comments = data.map(d => d.totalComments || 0);
    const satisfaction = data.map(d => d.avgSatisfaction || 0);


    if (charts.trend) {
        charts.trend.destroy();
    }

    charts.trend = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Comments',
                    data: comments,
                    borderColor: colors.primary,
                    backgroundColor: 'rgba(13, 110, 253, 0.1)',
                    borderWidth: 2,
                    fill: true,
                    tension: 0.4,
                    pointBackgroundColor: colors.primary,
                    pointBorderColor: '#fff',
                    pointBorderWidth: 2,
                    pointRadius: 5,
                    pointHoverRadius: 7,
                    yAxisID: 'y'
                },
                {
                    label: 'Avg Satisfaction',
                    data: satisfaction,
                    borderColor: colors.success,
                    backgroundColor: 'rgba(25, 135, 84, 0.1)',
                    borderWidth: 2,
                    fill: true,
                    tension: 0.4,
                    pointBackgroundColor: colors.success,
                    pointBorderColor: '#fff',
                    pointBorderWidth: 2,
                    pointRadius: 5,
                    pointHoverRadius: 7,
                    yAxisID: 'y1'
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: {
                    display: true,
                    position: 'top',
                    labels: {
                        padding: 15,
                        font: { size: 12 }
                    }
                },
                tooltip: {
                    backgroundColor: 'rgba(0, 0, 0, 0.8)',
                    padding: 12,
                    titleFont: { size: 13 },
                    bodyFont: { size: 12 },
                    mode: 'index',
                    intersect: false
                }
            },
            scales: {
                x: {
                    ticks: {
                        font: { size: 11 },
                        maxRotation: 45,
                        minRotation: 0
                    }
                },
                y: {
                    type: 'linear',
                    display: true,
                    position: 'left',
                    title: {
                        display: true,
                        text: 'Comments',
                        font: { size: 11, weight: 'bold' }
                    },
                    ticks: {
                        beginAtZero: true,
                        font: { size: 10 }
                    }
                },
                y1: {
                    type: 'linear',
                    display: true,
                    position: 'right',
                    title: {
                        display: true,
                        text: 'Satisfaction',
                        font: { size: 11, weight: 'bold' }
                    },
                    ticks: {
                        beginAtZero: true,
                        max: 100,
                        font: { size: 10 }
                    },
                    grid: { drawOnChartArea: false }
                }
            }
        }
    });
}


function initChannelChart(data) {
    const ctx = document.getElementById('channelChart');
    if (!ctx) return;


    const labels = data.map(d => d.channel || 'Unknown');
    const values = data.map(d => d.count || 0);
    const chartColors = [
        colors.primary,
        colors.success,
        colors.info,
        colors.warning,
        colors.danger
    ];


    if (charts.channel) {
        charts.channel.destroy();
    }

    charts.channel = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Opinions',
                data: values,
                backgroundColor: values.map((_, i) => chartColors[i % chartColors.length]),
                borderRadius: 4,
                borderSkipped: false
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    backgroundColor: 'rgba(0, 0, 0, 0.8)',
                    padding: 12,
                    titleFont: { size: 13 },
                    bodyFont: { size: 12 }
                }
            },
            scales: {
                x: {
                    beginAtZero: true,
                    ticks: {
                        font: { size: 10 }
                    }
                },
                y: {
                    ticks: {
                        font: { size: 11 }
                    }
                }
            }
        }
    });
}


function updateAllCharts(dashboardData) {
    if (dashboardData.sentiment) {
        initSentimentChart(dashboardData.sentiment);
    }
    if (dashboardData.topProducts) {
        initTopProductsChart(dashboardData.topProducts);
    }
    if (dashboardData.trend) {
        initTrendChart(dashboardData.trend);
    }
    if (dashboardData.channels) {
        initChannelChart(dashboardData.channels);
    }
}


async function refreshDashboard() {
    try {
        const response = await fetch('/api/dashboard/all');
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        
        const data = await response.json();
        updateAllCharts(data);
        console.log('Dashboard refreshed successfully');
    } catch (error) {
        console.error('Error refreshing dashboard:', error);
    }
}


setInterval(refreshDashboard, 5 * 60 * 1000);
