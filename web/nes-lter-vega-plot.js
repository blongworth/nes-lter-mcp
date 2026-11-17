/**
 * <nes-lter-vega-plot> renders a Vega-Lite specification.
 *
 * The host page must load vega, vega-lite, and vega-embed before this file:
 *   <script src="https://cdn.jsdelivr.net/npm/vega@5"></script>
 *   <script src="https://cdn.jsdelivr.net/npm/vega-lite@5"></script>
 *   <script src="https://cdn.jsdelivr.net/npm/vega-embed@6"></script>
 *
 * Usage:
 *   const plot = document.querySelector("nes-lter-vega-plot");
 *   plot.spec = vegaLiteSpec;
 */

class NesLterVegaPlot extends HTMLElement {
  static get observedAttributes() {
    return ["spec-url"];
  }

  constructor() {
    super();
    // Preserve properties assigned before the custom element is upgraded.
    // This happens when a deferred module is loaded after an inline script
    // assigns plot.spec during HTML parsing.
    const preUpgradeSpec = Object.prototype.hasOwnProperty.call(this, "spec")
      ? this.spec
      : null;
    if (preUpgradeSpec) delete this.spec;
    this._spec = null;
    this._shadow = this.attachShadow({ mode: "open" });
    if (preUpgradeSpec) this._spec = preUpgradeSpec;
  }

  set spec(value) {
    this._spec = value;
    this.render();
  }

  get spec() {
    return this._spec;
  }

  connectedCallback() {
    this.render();
  }

  attributeChangedCallback(name, oldValue, newValue) {
    if (name === "spec-url" && oldValue !== newValue) this.loadSpec(newValue);
  }

  async loadSpec(url) {
    if (!url) return;
    try {
      const response = await fetch(url);
      if (!response.ok) throw new Error(`Could not load ${url}: HTTP ${response.status}`);
      this.spec = await response.json();
    } catch (error) {
      this.showError(error);
    }
  }

  async render() {
    if (!this.isConnected) return;
    if (!this._spec && this.getAttribute("spec-url")) {
      await this.loadSpec(this.getAttribute("spec-url"));
      return;
    }
    if (!this._spec) return;
    if (typeof window.vegaEmbed !== "function") {
      this.showError(new Error("vegaEmbed is not loaded. Include vega, vega-lite, and vega-embed first."));
      return;
    }
    this._shadow.innerHTML = `
      <style>
        :host { display: block; min-height: 320px; font: 14px system-ui, sans-serif; }
        #plot { width: 100%; min-height: 320px; }
        .error { color: #a40000; padding: 1rem; border: 1px solid #e3b7b7; background: #fff7f7; }
      </style>
      <div id="plot" aria-label="Vega-Lite chart"></div>
    `;
    try {
      await window.vegaEmbed(this._shadow.querySelector("#plot"), this._spec, {
        actions: true,
        renderer: "canvas",
      });
    } catch (error) {
      this.showError(error);
    }
  }

  showError(error) {
    this._shadow.innerHTML = `<div class="error">${String(error.message || error)}</div>`;
  }
}

customElements.define("nes-lter-vega-plot", NesLterVegaPlot);
