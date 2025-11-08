"""
Reddit CS:GO/CS2 Gambling Research Scraper

Scrapes Reddit posts from CS:GO/CS2 and gaming subreddits about case opening,
gambling, loot boxes, and addiction. Exports data to CSV format.
"""

import praw
import pandas as pd
from datetime import datetime, timezone
import csv
import time
import logging
import json
from typing import List, Dict, Optional
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('reddit_scraper.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
CONFIG = {
    # Reddit API credentials (loaded from environment variables)
    'client_id': os.getenv('REDDIT_CLIENT_ID', ''),
    'client_secret': os.getenv('REDDIT_CLIENT_SECRET', ''),
    'user_agent': os.getenv('REDDIT_USER_AGENT', 'CSGamblingResearchScraper/1.0'),
    
    # Target subreddits
    'subreddits': [
        'GlobalOffensive',
        'csgo',
        'CS2',
        'gaming',
        'Steam'
    ],
    
    # Search keywords
    'keywords': [
        'case opening',
        'gambling',
        'loot box',
        'lootbox',
        'addiction',
        'skin gambling',
        'case unboxing',
        'csgo case',
        'cs2 case'
    ],
    
    # Date range (Unix timestamps)
    'start_date': datetime(2024, 11, 8, tzinfo=timezone.utc).timestamp(),
    'end_date': datetime(2025, 11, 8, tzinfo=timezone.utc).timestamp(),
    
    # Minimum upvotes filter
    'min_upvotes': 5,
    
    # Output file
    'output_file': 'reddit_cs_gambling_data.csv',
    
    # Rate limiting (seconds between requests)
    'request_delay': 1.0,
    
    # API retry settings
    'max_retries': 3,
    'retry_delay': 5.0  # seconds to wait before retrying
}


# Global Reddit instance (initialized in initialize_reddit_api)
reddit = None


def initialize_reddit_api():
    """
    Initialize and configure the Reddit API client.
    Uses read-only access (no user authentication required for public data).
    
    Returns:
        praw.Reddit: Initialized Reddit API client instance
        
    Raises:
        Exception: If Reddit API initialization fails
    """
    global reddit
    
    try:
        logger.info("Initializing Reddit API client...")
        
        reddit = praw.Reddit(
            client_id=CONFIG['client_id'],
            client_secret=CONFIG['client_secret'],
            user_agent=CONFIG['user_agent']
        )
        
        # Verify connection by checking read-only status
        if reddit.read_only:
            logger.info("Reddit API initialized successfully (read-only mode)")
        else:
            logger.warning("Reddit API initialized but not in read-only mode")
        
        # Test connection with a simple API call
        try:
            test_subreddit = reddit.subreddit('GlobalOffensive')
            _ = test_subreddit.display_name
            logger.info("Reddit API connection verified")
        except Exception as e:
            logger.error(f"Failed to verify Reddit API connection: {str(e)}")
            raise
        
        return reddit
        
    except Exception as e:
        logger.error(f"Failed to initialize Reddit API: {str(e)}")
        logger.error("Please check your credentials and internet connection")
        raise


def rate_limit_delay():
    """
    Implement rate limiting to respect Reddit's API limits.
    Reddit allows 60 requests per minute, so we add a delay between requests.
    """
    time.sleep(CONFIG['request_delay'])


def safe_api_call(func, *args, **kwargs):
    """
    Execute an API call with error handling and retry logic.
    
    Args:
        func: The function to execute (API call)
        *args: Positional arguments for the function
        **kwargs: Keyword arguments for the function
        
    Returns:
        Result of the API call function
        
    Raises:
        Exception: If all retry attempts fail
    """
    last_exception = None
    
    for attempt in range(CONFIG['max_retries']):
        try:
            rate_limit_delay()
            return func(*args, **kwargs)
            
        except praw.exceptions.RedditAPIException as e:
            # Reddit API specific errors
            error_message = str(e)
            logger.warning(f"Reddit API error (attempt {attempt + 1}/{CONFIG['max_retries']}): {error_message}")
            
            # Check for rate limiting
            if "RATE_LIMIT" in error_message.upper() or "429" in error_message:
                wait_time = CONFIG['retry_delay'] * (attempt + 1)
                logger.warning(f"Rate limit hit. Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            
            last_exception = e
            
        except praw.exceptions.ClientException as e:
            # Client-side errors (network, etc.)
            logger.warning(f"Client error (attempt {attempt + 1}/{CONFIG['max_retries']}): {str(e)}")
            last_exception = e
            time.sleep(CONFIG['retry_delay'])
            
        except Exception as e:
            # Other unexpected errors
            logger.error(f"Unexpected error (attempt {attempt + 1}/{CONFIG['max_retries']}): {str(e)}")
            last_exception = e
            time.sleep(CONFIG['retry_delay'])
    
    # All retries failed
    logger.error(f"Failed after {CONFIG['max_retries']} attempts")
    raise last_exception


def check_reddit_api_status():
    """
    Check if Reddit API is accessible and working.
    
    Returns:
        bool: True if API is working, False otherwise
    """
    try:
        if reddit is None:
            logger.error("Reddit API not initialized. Call initialize_reddit_api() first.")
            return False
        
        # Try a simple API call
        test_subreddit = reddit.subreddit('GlobalOffensive')
        _ = test_subreddit.display_name
        return True
        
    except Exception as e:
        logger.error(f"Reddit API status check failed: {str(e)}")
        return False


def matches_keywords(text: str, keywords: List[str]) -> bool:
    """
    Check if text contains any of the search keywords (case-insensitive).
    
    Args:
        text: Text to search in (post title, content, or comment)
        keywords: List of keywords to search for
        
    Returns:
        bool: True if any keyword is found, False otherwise
    """
    if not text:
        return False
    
    text_lower = text.lower()
    for keyword in keywords:
        if keyword.lower() in text_lower:
            return True
    return False


def is_in_date_range(post_timestamp: float) -> bool:
    """
    Check if a post's timestamp is within the configured date range.
    
    Args:
        post_timestamp: Unix timestamp of the post
        
    Returns:
        bool: True if post is within date range, False otherwise
    """
    return CONFIG['start_date'] <= post_timestamp <= CONFIG['end_date']


def meets_upvote_threshold(score: int) -> bool:
    """
    Check if a post meets the minimum upvote threshold.
    
    Args:
        score: Post score (upvotes)
        
    Returns:
        bool: True if score >= min_upvotes, False otherwise
    """
    return score >= CONFIG['min_upvotes']


def extract_post_data(submission) -> Optional[Dict]:
    """
    Extract relevant data from a Reddit submission (post).
    
    Args:
        submission: PRAW submission object
        
    Returns:
        dict: Dictionary containing post data, or None if post doesn't meet criteria
    """
    try:
        # Check if post meets basic criteria
        if not meets_upvote_threshold(submission.score):
            return None
        
        if not is_in_date_range(submission.created_utc):
            return None
        
        # Check if post matches keywords in title or content
        title_match = matches_keywords(submission.title, CONFIG['keywords'])
        content_match = matches_keywords(submission.selftext, CONFIG['keywords'])
        
        if not (title_match or content_match):
            return None
        
        # Extract top-level comments (limit to top 5 for performance)
        top_comments = []
        try:
            submission.comments.replace_more(limit=0)  # Remove "more comments" placeholders
            for comment in submission.comments.list()[:5]:  # Get top 5 comments
                if hasattr(comment, 'body') and comment.body and comment.body != '[deleted]':
                    if matches_keywords(comment.body, CONFIG['keywords']):
                        top_comments.append({
                            'author': str(comment.author) if comment.author else '[deleted]',
                            'body': comment.body[:500],  # Limit comment length
                            'score': comment.score,
                            'created_utc': comment.created_utc
                        })
        except Exception as e:
            logger.warning(f"Error extracting comments from post {submission.id}: {str(e)}")
        
        # Format comments as JSON string for CSV
        comments_json = json.dumps(top_comments, ensure_ascii=False) if top_comments else ""
        
        # Extract post data
        post_data = {
            'post_id': submission.id,
            'title': submission.title,
            'author': str(submission.author) if submission.author else '[deleted]',
            'content': submission.selftext[:2000] if submission.selftext else '',  # Limit content length
            'upvotes': submission.score,
            'timestamp': submission.created_utc,
            'date': datetime.fromtimestamp(submission.created_utc, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'subreddit': submission.subreddit.display_name,
            'url': f"https://www.reddit.com{submission.permalink}",
            'flair': submission.link_flair_text if submission.link_flair_text else '',
            'comment_count': submission.num_comments,
            'top_comments': comments_json
        }
        
        return post_data
        
    except Exception as e:
        logger.error(f"Error extracting data from post {submission.id}: {str(e)}")
        return None


def search_subreddit(subreddit_name: str, limit: int = 1000) -> List[Dict]:
    """
    Search a subreddit for posts matching keywords.
    
    Args:
        subreddit_name: Name of the subreddit to search
        limit: Maximum number of posts to retrieve (default: 1000)
        
    Returns:
        List[Dict]: List of post data dictionaries
    """
    if reddit is None:
        logger.error("Reddit API not initialized")
        return []
    
    posts_data = []
    
    try:
        logger.info(f"Searching r/{subreddit_name}...")
        subreddit = reddit.subreddit(subreddit_name)
        
        # Search using each keyword
        all_posts = set()  # Use set to avoid duplicates
        
        for keyword in CONFIG['keywords']:
            try:
                logger.info(f"  Searching for '{keyword}' in r/{subreddit_name}...")
                
                # Search posts by keyword
                def search_posts():
                    return list(subreddit.search(keyword, limit=limit, sort='new', time_filter='all'))
                
                search_results = safe_api_call(search_posts)
                
                for submission in search_results:
                    # Use submission ID as unique identifier
                    all_posts.add(submission.id)
                    
                    # Extract post data
                    post_data = extract_post_data(submission)
                    if post_data:
                        posts_data.append(post_data)
                        logger.info(f"    Found matching post: {post_data['title'][:50]}...")
                
            except Exception as e:
                logger.warning(f"Error searching for '{keyword}' in r/{subreddit_name}: {str(e)}")
                continue
        
        logger.info(f"Found {len(posts_data)} matching posts in r/{subreddit_name}")
        
    except Exception as e:
        logger.error(f"Error searching r/{subreddit_name}: {str(e)}")
    
    return posts_data


def save_to_csv(posts_data: List[Dict], filename: str = None):
    """
    Save collected post data to a CSV file.
    
    Args:
        posts_data: List of post data dictionaries
        filename: Output CSV filename (defaults to CONFIG['output_file'])
    """
    if not posts_data:
        logger.warning("No data to save")
        return
    
    if filename is None:
        filename = CONFIG['output_file']
    
    try:
        # Convert to DataFrame
        df = pd.DataFrame(posts_data)
        
        # Reorder columns for better readability
        column_order = [
            'post_id', 'title', 'author', 'content', 'upvotes', 'timestamp', 'date',
            'subreddit', 'url', 'flair', 'comment_count', 'top_comments'
        ]
        
        # Ensure all columns exist
        for col in column_order:
            if col not in df.columns:
                df[col] = ''
        
        df = df[column_order]
        
        # Check if file exists to append or create new
        file_exists = os.path.exists(filename)
        
        if file_exists:
            # Read existing data to avoid duplicates
            existing_df = pd.read_csv(filename)
            existing_ids = set(existing_df['post_id'].astype(str))
            
            # Filter out duplicates
            new_posts = [post for post in posts_data if str(post['post_id']) not in existing_ids]
            
            if new_posts:
                new_df = pd.DataFrame(new_posts)
                new_df = new_df[column_order]
                df = pd.concat([existing_df, new_df], ignore_index=True)
                logger.info(f"Appended {len(new_posts)} new posts to existing file")
            else:
                logger.info("No new posts to add (all duplicates)")
                return
        else:
            logger.info(f"Creating new CSV file: {filename}")
        
        # Save to CSV with UTF-8 encoding
        df.to_csv(filename, index=False, encoding='utf-8-sig')  # utf-8-sig for Excel compatibility
        logger.info(f"Saved {len(df)} posts to {filename}")
        
    except Exception as e:
        logger.error(f"Error saving to CSV: {str(e)}")
        raise


def main():
    """
    Main function to orchestrate the Reddit scraping process.
    """
    try:
        # Initialize Reddit API
        initialize_reddit_api()
        
        if not check_reddit_api_status():
            logger.error("Reddit API is not accessible. Exiting.")
            return
        
        logger.info("Starting Reddit scraping process...")
        logger.info(f"Target subreddits: {', '.join(CONFIG['subreddits'])}")
        logger.info(f"Search keywords: {', '.join(CONFIG['keywords'])}")
        logger.info(f"Date range: {datetime.fromtimestamp(CONFIG['start_date'], tz=timezone.utc).date()} to {datetime.fromtimestamp(CONFIG['end_date'], tz=timezone.utc).date()}")
        logger.info(f"Minimum upvotes: {CONFIG['min_upvotes']}")
        
        all_posts_data = []
        
        # Search each subreddit
        for subreddit_name in CONFIG['subreddits']:
            try:
                posts_data = search_subreddit(subreddit_name)
                all_posts_data.extend(posts_data)
            except Exception as e:
                logger.error(f"Failed to search r/{subreddit_name}: {str(e)}")
                continue
        
        # Remove duplicates based on post_id
        seen_ids = set()
        unique_posts = []
        for post in all_posts_data:
            if post['post_id'] not in seen_ids:
                seen_ids.add(post['post_id'])
                unique_posts.append(post)
        
        logger.info(f"Total unique posts collected: {len(unique_posts)}")
        
        # Save to CSV
        if unique_posts:
            save_to_csv(unique_posts)
            logger.info("Scraping process completed successfully!")
        else:
            logger.warning("No posts found matching the criteria")
        
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        raise


if __name__ == "__main__":
    main()

