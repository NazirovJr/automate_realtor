# Krisha.kz Automation Docker Setup

This repository contains a Docker Compose setup for automating Krisha.kz scraping and Telegram bot functionality.

## Components

1. **PostgreSQL Database**: Stores the scraped property data
2. **Krisha Crawler**: Scrapes Krisha.kz for new property listings
   - Runs once at startup
   - Then scheduled to run daily at 13:00 (1:00 PM)
3. **Telegram Bot**: Provides user interface for interacting with the scraped data
   - Runs continuously

## Usage

### Prerequisites

- Docker and Docker Compose installed
- Access to the source code for both the crawler and Telegram bot

### Starting the Services

To start all services, run:

```bash
docker-compose up -d
```

This will:
- Start the PostgreSQL database
- Build and start the crawler (which will run immediately and then daily at 1:00 PM)
- Build and start the Telegram bot (which will run continuously)

### Viewing Logs

#### Crawler logs:

```bash
docker logs krisha-crawler
```

#### Telegram bot logs:

```bash
docker logs krisha-telegram
```

### Stopping the Services

To stop all services:

```bash
docker-compose down
```

To stop all services and remove volumes (including database data):

```bash
docker-compose down -v
```

## Maintenance

### Updating the Services

If you need to update the code:

1. Make changes to the source code
2. Rebuild and restart the services:

```bash
docker-compose up -d --build
```

### Managing the Database

To connect to the PostgreSQL database:

```bash
docker exec -it krisha-db psql -U postgres -d krisha
```

## Troubleshooting

- If the crawler isn't running on schedule, check the cron logs:
  ```bash
  docker exec -it krisha-crawler cat /app/cron.log
  ```

- To manually trigger the crawler:
  ```bash
  docker exec -it krisha-crawler python -m src.krisha.main
  ``` 