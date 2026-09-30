# Binquant Repository Instructions

## Strategy discovery boundary

- Keep `strategies/` exclusively for concrete trading strategy implementations that should be discoverable by the frontend algorithm-ranking component.
- Do not place generic helpers, shared calculations, data models, constants, mixins, or other support modules in `strategies/`; put reusable strategy support code in `shared/` instead.
- A module belongs in `strategies/` only when it represents an algorithm that can emit or evaluate a named trading signal.
- Before adding a file to `strategies/`, consider whether frontend discovery should expose it as an algorithm. If not, place it elsewhere.

## Validation

- Update focused regression tests whenever trading calculations, signal routing, or Telegram output changes.
- Run `make format` and `make test` before handing off completed changes.
