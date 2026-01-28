/**
 * Theme: Dastone - Bootstrap 5 Responsive Admin Dashboard
 * Author: Mannatthemes
 * Analytics Dashboard Js
 */



// Audience Overview

var options = {
  series: [{
      data: series.monthDataSeries1.prices
  }],
  chart: {
      height: 275,
      type: "area",
      toolbar: {
          show: false,
      },
      dropShadow: {
          enabled: true,
          top: 12,
          left: 0,
          bottom: 0,
          right: 0,
          blur: 2,
          color: "rgba(132, 145, 183, 0.3)",
          opacity: 0.35,
      },
  },
  annotations: {
      xaxis: [{
          x: 270,
          strokeDashArray: 4,
          borderWidth: 1,
          borderColor: ["var(--bs-secondary)"],
      }, ],
      points: [{
          x: 270,
          y: 40,
          marker: {
              size: 6,
              fillColor: ["var(--bs-primary)"],
              strokeColor: ["var(--bs-card-bg)"],
              strokeWidth: 4,
              radius: 5,
          },
          label: {
              borderWidth: 1,
              offsetY: -55,
              text: '50k',
              style: {
                  background: ["var(--bs-primary)"],
                  fontSize: '14px',
                  fontWeight: '600',
              }
          }
      }],
  },
  colors: ["#06afdd", "#f4a14d"],
  dataLabels: {
      enabled: false,
  },
  stroke: {
      show: true,
      curve: "straight",
      width: [2, 2],
      dashArray: [0, 0],
      lineCap: "round",
  },
  series: [{
          name: "Income",
          data: [40, 40, 30, 30, 10, 10, 10, 30, 30, 40, 40, 51],
      },
      {
          name: "Expenses",
          data: [20, 20, 10, 10, 40, 40, 60, 60, 20, 20, 20, 40],
      },
  ],
  labels: [
      "Jan",
      "Feb",
      "Mar",
      "Apr",
      "May",
      "Jun",
      "Jul",
      "Aug",
      "Sep",
      "Oct",
      "Nov",
      "Dec",
  ],

  yaxis: {
      labels: {
          offsetX: -12,
          offsetY: 0,
          formatter: function (value) {
              return "$" + value;
          }
      },
  },
  grid: {
      strokeDashArray: 3,
      xaxis: {
          lines: {
              show: true,
          },
      },
      yaxis: {
          lines: {
              show: false,
          },
      },
  },
  legend: {
      show: false,
  },

  fill: {
      type: "gradient",
      gradient: {
          type: "vertical",
          shadeIntensity: 1,
          inverseColors: !1,
          opacityFrom: 0.05,
          opacityTo: 0.05,
          stops: [45, 100],
      },
  },
};

var chart = new ApexCharts(document.querySelector("#ana_dash_1"), options);
chart.render();
  
   //customers-widget
  
   
   var options = {
    chart: {
        height: 250,
        type: 'donut',
    }, 
    plotOptions: {
      pie: {
        donut: {
          size: '80%'
        }
      }
    },
    dataLabels: {
      enabled: false,
    },
  
    stroke: {
      show: true,
      width: 2,
      colors: ['transparent']
    },
   
    series: [50, 25, 25,],
    legend: {
      show: true,
      position: 'bottom',
      horizontalAlign: 'center',
      verticalAlign: 'middle',
      floating: false,
      fontSize: '13px',
      fontFamily: "Be Vietnam Pro, sans-serif",
      offsetX: 0,
      offsetY: 0,
    },
    labels: [ "Currenet","New", "Retargeted" ],
    colors: ["#6f6af8", "#08b0e7", "#f4a14d"],
   
    responsive: [{
        breakpoint: 600,
        options: {
          plotOptions: {
              donut: {
                customScale: 0.2
              }
            },        
            chart: {
                height: 240
            },
            legend: {
                show: false
            },
        }
    }],
    tooltip: {
      y: {
          formatter: function (val) {
              return   val + " %"
          }
      }
    }
    
  }
  
  var chart = new ApexCharts(
    document.querySelector("#sessions_device"),
    options
  );
  
  chart.render();




  var options = {
    chart: {
        height: 315,
        type: 'area',
        width: '100%',
        stacked: true,
        toolbar: {
          show: false,
          autoSelected: 'zoom'
        },
    },
    colors: ['#2a77f4', 'rgba(42, 118, 244, .4)'],
    dataLabels: {
        enabled: false
    },
    stroke: {
        curve: 'straight',
        width: [0, 0],
        dashArray: [0, 4],
        lineCap: 'round',
    },
    grid: {
      padding: {
        left: 0,
        right: 0
      },
      strokeDashArray: 3,
    },
    markers: {
      size: 0,
      hover: {
        size: 0
      }
    },
    series: [{
        name: 'New Visits',
        data: [0,40,90,40,50,30,35,20,10,0,0,0]
    }, {
        name: 'Unique Visits',
        data: [20,80,120,60,70,50,55,40,50,30,35,0]
    }],
  
    xaxis: {
        type: 'month',
        categories: ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
        axisBorder: {
          show: true,
        },  
        axisTicks: {
          show: true,
        },                  
    },
    fill: {
      type: "gradient",
      gradient: {
        shadeIntensity: 1,
        opacityFrom: 1,
        opacityTo: 1,
        stops: [100]
      }
    },
    
    tooltip: {
        x: {
            format: 'dd/MM/yy HH:mm'
        },
    },
    legend: {
      position: 'top',
      horizontalAlign: 'right'
    },
  }
  
  var chart = new ApexCharts(
    document.querySelector("#monthly_income"),
    options
  );
  
  chart.render();

