"""
Lambda Function Code that goes inside the AWS Console 
with its Layer with dependencies
"""

from datetime import datetime 
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy_playwright.page import PageMethod
import os
import json

# Log Folder
os.makedirs('logs', exist_ok=True) 

# Current time for naming
now = datetime.today().strftime("%Y-%m-%d_%H-%M-%S")

# Outputs names
FILENAME = f"{now}_items.jsonl"
BUCKET_URI = f"s3://amzn-website-scraper/00-layer-ingestion/{FILENAME}" 

# Main Spider Class
class AmazonSpider(scrapy.Spider):
    name = "amazon"
    allowed_domains = ["www.amazon.com.br"]
    start_url = "https://www.amazon.com.br/blackfriday" 
    now = datetime.now().strftime(format="%Y-%m-%d_%H:%M:%S") 

    def start_requests(self):
        yield scrapy.Request(
            url=self.start_url,
            meta={
                "playwright": True,
                "playwright_include_page": True,  # Pass the Page object to parse
                "playwright_page_methods": [
                    PageMethod("wait_for_selector", "div.GridItem-module__container_PW2gdkwTj1GQzdwJjejN"),
                ],
            },
            callback=self.parse
        )

    async def parse(self, response):
        page = response.meta["playwright_page"]
        scroll_attempts = 0
        load_more_clicks = 0
        max_scrolls = 50  # Safety limit
        
        while scroll_attempts < max_scrolls:
            
            # Extract currently visible items
            items = response.css("div.GridItem-module__container_PW2gdkwTj1GQzdwJjejN")

            for item in items:
                item_id = item.css("div.ProductCard-module__card_uyr_Jh7WpSkPx4iEpn4w::attr(data-csa-c-item-id)").get()
                description = item.css("span.a-truncate-cut::text").get() 
                current_price = item.css("div.ProductCard-module__priceToPay_olAgJzVNGyj2javg2pAe span.a-offscreen::text").get()
                discount = item.css("div.style_filledRoundedBadgeLabel__Vo-4g span.a-size-mini::text").get()
                previous_price = item.css("div.ProductCard-module__wrapPrice__sMO92NjAjHmGPn3jnIH span.a-offscreen::text").get()  
                url_href = item.css("a.ProductCard-module__cardContainingLink_OkTYz2ZO_0za69shksJ7::attr(href)").get()

                if description:
                    yield {
                        "item_id": item_id,
                        "product-description": description, 
                        "url": url_href,
                        "price": current_price,
                        "previous_price": previous_price,
                        "discount": discount,
                        "created_at": self.now
                    }
            
            # Scroll down 500px
            await page.evaluate("window.scrollBy(0, 800)")
            await page.wait_for_timeout(500)  # Wait 0.5s for content to load
            
            # Check if we've reached the bottom
            is_at_bottom = await page.evaluate("""
                () => {
                    const scrollTop = window.scrollY;
                    const scrollHeight = document.documentElement.scrollHeight;
                    const clientHeight = document.documentElement.clientHeight;
                    return scrollTop + clientHeight >= scrollHeight - 100;
                }
            """)
            
            # Check if 'Load More' button exists
            load_more_button = await page.query_selector('[data-testid="load-more-view-more-button"]')
            
            if load_more_button:
                self.logger.info("'Load More' button found - clicking it")
                await page.click('[data-testid="load-more-view-more-button"]')
                load_more_clicks += 1
                await page.wait_for_timeout(1500)  # Wait for new items to load
                
                # Update response with new content
                html_content = await page.content()
                response = scrapy.http.HtmlResponse(
                    url=page.url,
                    body=html_content.encode('utf-8'),
                    encoding='utf-8'
                )
                # Skip the normal response update below since we just did it
                scroll_attempts += 1
                continue
            
            # Check if we should stop: at bottom, clicked button at least once, and no more button
            if is_at_bottom and load_more_clicks > 1 and not load_more_button:
                self.logger.info(f"Finished scraping: at bottom, clicked 'Load More' {load_more_clicks} times, no more button available")
                break
            
            # Update response with new page content
            response = await page.content()
            response = scrapy.http.HtmlResponse(
                url=page.url,
                body=response.encode('utf-8'),
                encoding='utf-8'
            )
            
            scroll_attempts += 1 

# API Spider instantiation
process = CrawlerProcess(
    settings={
        "FEEDS": {
            BUCKET_URI : {"format": "jsonlines"}
        }
    }
)

# Lambda Function
def lambda_handler(event, context):
    try:
        process.crawl(AmazonSpider) 
        process.start() 
    except Exception as e:
        print(e)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
