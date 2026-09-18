import nodriver as uc
import asyncio


async def main():
    browser = await uc.start(
        headless=True,
        sandbox=False,
        browser_args=[
            "--user-agent=Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/137.0.0.0 Safari/537.36",
            "--disable-gpu",
            "--window-size=1920,1080",
        ],
    )
    print("Browser started", flush=True)
    try:
        tab = await browser.get("https://www.google.com/maps/search/media+modeling+agency+dhaka/")
        print("Tab obtained", flush=True)

        # Smart poll: wait for result links, up to 60s
        for i in range(12):
            await asyncio.sleep(5)
            try:
                links = await tab.evaluate("document.querySelectorAll('a.hfpxzc').length")
                print(f"t={(i+1)*5}s: links={links}", flush=True)
                if links and int(links) > 0:
                    break
            except Exception as e:
                print(f"t={(i+1)*5}s: error: {type(e).__name__}", flush=True)

        # Extract first card's full text content to understand the data layout
        card_text = await tab.evaluate("""
            (() => {
                const feed = document.querySelector('div[role="feed"]');
                if (!feed || !feed.children[0]) return 'NO_FEED';
                return feed.children[0].textContent.substring(0, 800);
            })()
        """)
        print(f"Card 0 text: {card_text}", flush=True)

        # Extract second card too for comparison
        card1_text = await tab.evaluate("""
            (() => {
                const feed = document.querySelector('div[role="feed"]');
                if (!feed || !feed.children[1]) return 'NO_CARD';
                return feed.children[1].textContent.substring(0, 800);
            })()
        """)
        print(f"Card 1 text: {card1_text}", flush=True)

        # Extract structured data from first 3 cards using a single JS call
        structured = await tab.evaluate("""
            (() => {
                const feed = document.querySelector('div[role="feed"]');
                if (!feed) return [];
                const results = [];
                for (let i = 0; i < Math.min(feed.children.length, 3); i++) {
                    const card = feed.children[i];
                    const link = card.querySelector('a.hfpxzc');
                    
                    // Rating: look for role=img with star info
                    const ratingEl = card.querySelector('[role="img"]');
                    const ratingStr = ratingEl?.getAttribute('aria-label') || '';
                    
                    // Category and address are in the text content sections
                    // Get all span elements with class info
                    const spans = [...card.querySelectorAll('span')].map(s => s.textContent?.trim()).filter(Boolean);
                    
                    // Phone: look for tel: link
                    const phoneLink = card.querySelector('a[href^="tel:"]');
                    const phone = phoneLink?.textContent?.trim() || phoneLink?.href?.replace('tel:', '') || '';
                    
                    // Website: look for data-tooltip="Open website" or similar
                    const websiteLink = card.querySelector('a[data-tooltip="Open website"]');
                    const website = websiteLink?.href || '';
                    
                    // Hours
                    const hoursEl = card.querySelector('[data-tooltip]');
                    const hours = hoursEl?.getAttribute('data-tooltip') || '';
                    
                    results.push({
                        name: link?.getAttribute('aria-label') || '',
                        url: link?.href || '',
                        ratingStr: ratingStr,
                        phone: phone,
                        website: website,
                        hoursTooltip: hours,
                        spanCount: spans.length,
                        spansSample: spans.slice(0, 15),
                    });
                }
                return results;
            })()
        """)
        print(f"Structured: {structured}", flush=True)

    except Exception as e:
        print(f"Error: {type(e).__name__}: {e}", flush=True)
    browser.stop()
    print("Done", flush=True)


uc.loop().run_until_complete(main())
