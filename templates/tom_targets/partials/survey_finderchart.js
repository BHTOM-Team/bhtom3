(function () {
  'use strict';

  function toDegrees(value, unit) {
    return unit === 'arcsec' ? value / 3600 : unit === 'arcmin' ? value / 60 : value;
  }

  function ChartText(x, y, value, options) {
    this.x = x;
    this.y = y;
    this.text = value;
    this.color = options.color;
    this.align = options.align || 'center';
    this.baseline = options.baseline || 'alphabetic';
  }

  ChartText.prototype.setOverlay = function (overlay) {
    this.overlay = overlay;
  };

  ChartText.prototype.draw = function (ctx) {
    ctx.fillStyle = this.color;
    ctx.font = '15px Arial';
    ctx.textAlign = this.align;
    ctx.textBaseline = this.baseline;
    ctx.fillText(this.text, this.x, this.y);
  };

  function bootstrap() {
    const chart = document.querySelector('.js-survey-finderchart');
    if (!chart) return;
    const chartView = document.getElementById('aladin-lite-div');
    if (!window.A || typeof A.aladin !== 'function') {
      chartView.textContent = 'Sky view could not load. Please reload the page.';
      return;
    }

    const ra = Number(chart.dataset.targetRa);
    const dec = Number(chart.dataset.targetDec);
    if (!Number.isFinite(ra) || !Number.isFinite(dec)) return;

    const overlays = JSON.parse(document.getElementById('survey-finderchart-overlays').textContent);
    const fieldSize = document.getElementById('fov');
    const fieldUnits = document.getElementById('fov-units-select');
    const scaleSize = document.getElementById('scale-bar-size');
    const scaleUnits = document.getElementById('scale-bar-units-select');
    const fieldDegrees = () => toDegrees(Number(fieldSize.value), fieldUnits.value);

    try {
      const spinner = document.getElementById('aladin-spinner');
      if (spinner) spinner.style.display = 'none';

      const aladin = A.aladin('#aladin-lite-div', {
        survey: 'P/DSS2/color',
        fov: fieldDegrees(),
        showReticle: false,
        target: String(ra) + ' ' + String(dec),
        showLayersControl: true,
        showGotoControl: false,
        showZoomControl: false
      });

      const annotationLayer = A.graphicOverlay({name: 'chart annotations', color: '#f72525', lineWidth: 2});
      aladin.addOverlay(annotationLayer);

      // One graphic layer per service gives each radius its own checkbox in
      // Aladin's Overlay layers menu. Keep them independent of chart annotations.
      overlays.forEach(function (item) {
        const layer = A.graphicOverlay({name: item.name, color: item.color, lineWidth: 2});
        aladin.addOverlay(layer);
        layer.add(A.circle(ra, dec, item.radius_arcsec / 3600));
        layer.hide();
        const show = layer.show.bind(layer);
        layer.show = function () {
          const diameter = item.radius_arcsec * 2.2 / 3600;
          if (aladin.getFov()[0] < diameter) {
            aladin.setFov(diameter);
            fieldUnits.value = 'deg';
            fieldSize.value = Number(diameter.toPrecision(3));
          }
          show();
        };
      });

      function annotate() {
        const fov = aladin.getFov()[0];
        const size = aladin.getSize();
        const offset = 30;
        const cosDec = Math.max(0.001, Math.cos(dec * Math.PI / 180));
        const compassPixel = [size[0] - offset, size[1] - offset];
        const scalePixel = [offset, size[1] - offset];
        const compass = aladin.pix2world(compassPixel[0], compassPixel[1]);
        const north = [compass[0], compass[1] + fov / 10];
        const east = [compass[0] + fov / (10 * cosDec), compass[1]];
        const northPixel = aladin.world2pix(north[0], north[1]);
        const eastPixel = aladin.world2pix(east[0], east[1]);
        const scale = Math.max(0, Number(scaleSize.value));
        const scaleLabel = String(scale) + ' ' + scaleUnits.value;
        const scaleStart = aladin.pix2world(scalePixel[0], scalePixel[1]);
        const scaleEnd = [scaleStart[0] - toDegrees(scale, scaleUnits.value) / cosDec, scaleStart[1]];
        const scaleEndPixel = aladin.world2pix(scaleEnd[0], scaleEnd[1]);
        const scaleLength = Math.abs(scaleEndPixel[0] - scalePixel[0]);
        const color = '#f72525';

        annotationLayer.removeAll();
        annotationLayer.add(A.polyline([north, compass, east]));
        annotationLayer.add(A.polyline([scaleStart, scaleEnd]));
        annotationLayer.add(A.circle(ra, dec, fov / 30));
        annotationLayer.add(new ChartText(scalePixel[0] + scaleLength / 2, scalePixel[1] - 7, scaleLabel, {color: color}));
        annotationLayer.add(new ChartText(northPixel[0], northPixel[1] - 3, 'N', {color: color}));
        annotationLayer.add(new ChartText(eastPixel[0] - 3, eastPixel[1], 'E', {color: color, align: 'end', baseline: 'middle'}));
      }

      aladin.on('positionChanged', annotate);
      aladin.on('zoomChanged', annotate);
      annotate();

      document.getElementById('update-finderchart').addEventListener('click', function () {
        const fov = fieldDegrees();
        if (Number.isFinite(fov) && fov > 0) {
          aladin.setFov(fov);
          annotate();
        }
      });
      document.getElementById('download-chart').addEventListener('click', function () {
        this.href = aladin.getViewDataURL();
      });
    } catch (error) {
      chartView.textContent = 'Sky view could not load. Please reload the page.';
      console.error('Aladin finding chart failed to initialize:', error);
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', bootstrap);
  } else {
    bootstrap();
  }
})();
