# plugin-experiment-dashboard

Superset chart plugin for A/B experiment analysis. Provides two views:

- **List view** — Searchable, sortable table of experiments with status, exposure counts, group allocation, and links to per-experiment charts.
- **Detail view** — Full funnel analysis for a single experiment: traffic balance check, variant comparison with confidence intervals, frequentist and Bayesian statistics.

### Setup

The plugin is registered as a workspace dependency in `superset-frontend/package.json`. No extra install steps needed — it's built as part of the standard Superset frontend build.

The plugin is registered in `superset-frontend/src/visualizations/presets/MainPreset.js`:

```js
import { ExperimentDashboardPlugin } from '@superset-ui/plugin-experiment-dashboard';

new ExperimentDashboardPlugin().configure({ key: 'experiment_ab' }),
```

### Development

From `superset-frontend/`:

```
npm run dev-server
```
