# Mint Bot — Ready Package

This package is based on the latest compatible TURBO/Eligible Free-Mint source in the project files and includes Docker, Compose, Railway, persistent data directory support, and offline structural tests.

## Local
1. `cp .env.example .env`
2. Fill the environment variables.
3. `python test_all.py`
4. `python -m unittest discover -v`
5. `python main.py`

## Docker
1. `cp .env.example .env`
2. `docker compose build`
3. `docker compose up -d`
4. `docker compose logs -f mint-bot`

## Railway
The included `railway.toml` uses NIXPACKS and starts `python3 main.py`. For persistent history, add a Railway Volume mounted at `/app/data` if the deployment needs filesystem persistence.

## Fast behavior
The source starts in `ultra` fast-first mode by default. It uses the fixed gas limit and fast broadcast path, while retaining wallet ownership, mint-history, and on-chain price checks as safeguards. Set `BOT_ENABLED = False` in `main.py` before any dry run that must not submit transactions.
