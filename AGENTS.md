# Binquant Repository Instructions

## Python packages

- Use implicit namespace packages, consistent with existing application folders.
- Do not add `__init__.py` merely to mark a directory as a package or hold a docstring. Add it only when required for package initialization, intentional exports, tooling, or import resolution.

## Docker Compose

- Change Compose files only when absolutely necessary; every Compose change requires a corresponding update to the separately maintained production `compose.yml`.
- Whenever you change a Compose file, explicitly prompt the user to update the production `compose.yml` with the matching changes. Do not assume local Compose changes are applied to production automatically.

## Strategy discovery boundary

- Keep `strategies/` exclusively for concrete trading strategy implementations that should be discoverable by the frontend algorithm-ranking component.
- Do not place generic helpers, shared calculations, data models, constants, mixins, or other support modules in `strategies/`; put reusable strategy support code in `shared/` instead.
- A module belongs in `strategies/` only when it represents an algorithm that can emit or evaluate a named trading signal.
- Before adding a file to `strategies/`, consider whether frontend discovery should expose it as an algorithm. If not, place it elsewhere.
- Keep notification-only alerts, pattern notifiers, and Telegram delivery in `notifiers/`. Reusable pattern calculations that do not send notifications belong in `shared/`.

## Legacy market context

- Treat `LiveMarketContext` as legacy compatibility infrastructure. Do not introduce new strategy, routing, notification, or signal-persistence dependencies on it.
- New strategy context must come from `ContextEvaluator` and its shared accessors.
- For regime information, use `macroregime_directional`, `macroregime_oscillation_intensity`, `microregime_directional`, and `microregime_oscillation_intensity` through `ContextEvaluator`.
- Existing `LiveMarketContext` consumers may remain until they are migrated, but do not expand the model or use its legacy `market_regime`, transition, or micro-regime fields in new work.

## Validation

- Update focused regression tests whenever trading calculations, signal routing, or Telegram output changes.
- Run `make format` and `make test` before handing off completed changes.
