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

## Safety behavior
The included source is the compatible free-mint/eligibility version: it validates wallet eligibility before transaction construction and only announces confirmed mined success. Review `.env.example` before enabling live operation.
