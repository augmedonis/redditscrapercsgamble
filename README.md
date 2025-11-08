# Reddit CS:GO/CS2 Gambling Research Scraper

A Python script to scrape Reddit posts from CS:GO/CS2 and gaming subreddits about case opening, gambling, loot boxes, and addiction. Data is collected from November 2024 to November 2025 and exported to CSV format.

## Features

- Searches multiple subreddits (GlobalOffensive, csgo, CS2, gaming, Steam)
- Filters posts by keywords related to gambling and case opening
- Date range filtering (Nov 2024 - Nov 2025)
- Minimum upvote threshold (5+ upvotes)
- Extracts post data including top comments
- Exports to CSV with UTF-8 encoding
- Rate limiting and error handling
- Duplicate detection

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Reddit API Credentials

1. Create a Reddit application at https://www.reddit.com/prefs/apps
2. Copy `.env.example` to `.env`:
   ```bash
   cp .env.example .env
   ```
3. Edit `.env` and add your Reddit API credentials:
   ```
   REDDIT_CLIENT_ID=your_client_id_here
   REDDIT_CLIENT_SECRET=your_client_secret_here
   REDDIT_USER_AGENT=CSGamblingResearchScraper/1.0 by /u/your_username
   ```

### 3. Run the Scraper

```bash
python reddit-scraper.py
```

## Configuration

You can modify the following in `reddit-scraper.py`:

- **Subreddits**: Edit the `subreddits` list in `CONFIG`
- **Keywords**: Edit the `keywords` list in `CONFIG`
- **Date Range**: Modify `start_date` and `end_date` in `CONFIG`
- **Minimum Upvotes**: Change `min_upvotes` in `CONFIG`
- **Output File**: Change `output_file` in `CONFIG`

## Output

The script generates:
- `reddit_cs_gambling_data.csv` - Main data file with all collected posts
- `reddit_scraper.log` - Log file with scraping progress and errors

## CSV Columns

- `post_id` - Reddit post ID
- `title` - Post title
- `author` - Post author
- `content` - Post content (first 2000 chars)
- `upvotes` - Post score/upvotes
- `timestamp` - Unix timestamp
- `date` - Human-readable date
- `subreddit` - Subreddit name
- `url` - Full Reddit URL
- `flair` - Post flair
- `comment_count` - Number of comments
- `top_comments` - Top 5 relevant comments (JSON format)

## Notes

- The script respects Reddit's API rate limits (60 requests/minute)
- Duplicate posts are automatically filtered out
- The script can be run multiple times - it will append new posts to existing CSV
- Logs are saved to `reddit_scraper.log` for debugging

## License

This project is for research purposes only. Please respect Reddit's Terms of Service and API usage guidelines.

