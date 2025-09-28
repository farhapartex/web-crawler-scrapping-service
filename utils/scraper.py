import time
import logging
import requests
from typing import List, Optional, Dict, Any
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from config.settings import settings

logger = logging.getLogger(__name__)


class BookScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

    def get_page_content(self, url: str) -> Optional[BeautifulSoup]:
        try:
            response = self.session.get(
                url,
                timeout=settings.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except Exception as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None

    def extract_books_from_page(self, soup: BeautifulSoup, page_url: str, base_url: str) -> List[Dict[str, Any]]:
        books = []
        try:
            book_articles = soup.find_all('article', class_='product_pod')

            for article in book_articles:
                book = self._extract_book_data(article, page_url, base_url)
                if book:
                    books.append(book)

            logger.info(f"Extracted {len(books)} books from {page_url}")
        except Exception as e:
            logger.error(f"Failed to extract books from {page_url}: {e}")

        return books

    def _extract_book_data(self, article, page_url: str, base_url: str) -> Optional[Dict[str, Any]]:
        try:
            book = {}

            img_tag = article.find('img')
            if img_tag:
                book['image_url'] = urljoin(base_url, img_tag.get('src', ''))

            title_tag = article.find('h3')
            if title_tag:
                link_tag = title_tag.find('a')
                if link_tag:
                    book['book_title'] = link_tag.get('title', '').strip()
                    book['book_url'] = urljoin(page_url, link_tag.get('href', ''))

            price_tag = article.find('p', class_='price_color')
            if price_tag:
                book['price'] = price_tag.text.strip()

            stock_tag = article.find('p', class_='instock')
            if stock_tag:
                book['stock_status'] = stock_tag.text.strip().replace('\n', ' ')

            rating_tag = article.find('p', class_=lambda x: x and x.startswith('star-rating'))
            if rating_tag:
                rating_classes = rating_tag.get('class', [])
                rating = next((cls for cls in rating_classes if cls in ['One', 'Two', 'Three', 'Four', 'Five']), 'Unknown')
                book['rating'] = rating

            book['page_url'] = page_url

            if all(key in book for key in ['book_title', 'book_url', 'image_url']):
                return book

        except Exception as e:
            logger.error(f"Failed to extract book data: {e}")

        return None

    def get_next_page_url(self, soup: BeautifulSoup, current_url: str) -> Optional[str]:
        try:
            next_link = soup.find('li', class_='next')
            if next_link:
                next_a = next_link.find('a')
                if next_a:
                    href = next_a.get('href')
                    if href:
                        return urljoin(current_url, href)
        except Exception as e:
            logger.error(f"Failed to get next page URL: {e}")

        return None

    def extract_book_details(self, book_url: str) -> Optional[Dict[str, Any]]:
        try:
            soup = self.get_page_content(book_url)
            if not soup:
                return None

            details = {}

            # Extract title
            title_tag = soup.find('h1')
            if title_tag:
                details['title'] = title_tag.text.strip()

            # Extract data from product information table
            price_table = soup.find('table', class_='table-striped')
            if price_table:
                for row in price_table.find_all('tr'):
                    th = row.find('th')
                    td = row.find('td')
                    if th and td:
                        key = th.text.strip().lower()
                        value = td.text.strip()

                        if 'price (excl. tax)' in key:
                            details['price_excl_tax'] = value
                        elif 'price (incl. tax)' in key:
                            details['price_incl_tax'] = value
                        elif 'availability' in key:
                            details['availability'] = value
                        elif 'product type' in key:
                            details['product_type'] = value
                        elif 'upc' in key:
                            details['upc'] = value
                        elif 'tax' in key and 'price' not in key:
                            details['tax'] = value
                        elif 'number of reviews' in key:
                            details['number_of_reviews'] = value

            # Extract star rating
            star_rating = soup.find('p', class_=lambda x: x and 'star-rating' in x)
            if star_rating:
                rating_classes = star_rating.get('class', [])
                rating_word = next((cls for cls in rating_classes if cls in ['One', 'Two', 'Three', 'Four', 'Five']), 'Zero')
                rating_map = {'Zero': 0, 'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5}
                details['star_count'] = rating_map.get(rating_word, 0)

            # Extract description
            description_div = soup.find('div', id='product_description')
            if description_div:
                next_p = description_div.find_next_sibling('p')
                if next_p:
                    details['description'] = next_p.text.strip()

            # Extract image URL - fix the selector
            img_container = soup.find('div', class_='item active')
            if img_container:
                img_tag = img_container.find('img')
                if img_tag:
                    details['image_url'] = urljoin(book_url, img_tag.get('src', ''))

            # Extract stock status from the main product area
            stock_tag = soup.find('p', class_='instock availability')
            if stock_tag:
                # Clean up the stock status text
                stock_text = stock_tag.get_text(strip=True)
                details['stock_status'] = stock_text

            # Extract price from main product area as backup
            if 'price_excl_tax' not in details:
                price_tag = soup.find('p', class_='price_color')
                if price_tag:
                    details['price_color'] = price_tag.text.strip()

            logger.info(f"Extracted details for book: {details.get('title', 'Unknown')}")
            return details

        except Exception as e:
            logger.error(f"Failed to extract book details from {book_url}: {e}")
            return None

    def delay_request(self):
        time.sleep(settings.REQUEST_DELAY)


scraper = BookScraper()