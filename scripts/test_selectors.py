import asyncio, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from base.browser_manager import BrowserManager

async def main():
    bm = BrowserManager(headless=True, page_wait_seconds=4.0, pinchtab_config={"instance_url": "http://127.0.0.1:9868", "server_url": "http://127.0.0.1:9867", "token": "123456"})
    await bm.start()

    url = "https://www.google.com/maps/search/North+End+Coffee+Roasters+Dhaka/@23.7528068,90.3744336,17z?hl=en"
    tab = await bm.navigate(url)
    await asyncio.sleep(10)

    # Dump the FULL HTML of the first 3 search result cards
    cards_html = await tab.evaluate("""(() => {
        const cards = document.querySelectorAll('[role="article"]');
        return Array.from(cards).slice(0, 2).map(c => c.outerHTML);
    })()""")
    for i, html in enumerate(cards_html):
        print(f"\n=== CARD {i} HTML ===")
        print(html[:3000])
        print("...")

    # Specifically look at phone number structure in the first card
    phone_data = await tab.evaluate("""(() => {
        const card = document.querySelector('[role="article"]');
        if (!card) return 'no card';
        // Find the element containing the phone
        const text = card.innerText;
        const phoneMatch = text.match(/(\\+?\\d[\\d\\s-]{7,}\\d)/);
        // Find all button and anchor elements
        const interactive = Array.from(card.querySelectorAll('button, a')).map(el => ({
            tag: el.tagName, href: (el.href||'').substring(0,80),
            text: (el.textContent||'').trim().substring(0,40),
            cls: el.className.substring(0,40),
            ariaLabel: el.getAttribute('aria-label')||'',
            jslog: el.getAttribute('jslog')||''
        }));
        return JSON.stringify({ phoneMatch, interactive });
    })()""")
    print(f"\nPhone + interactive elements:\n{phone_data}")

    # Check the class structure of the card
    card_structure = await tab.evaluate("""(() => {
        const card = document.querySelector('[role="article"]');
        if (!card) return 'no card';
        const walk = (el, depth) => {
            if (depth > 6 || !el) return null;
            const info = {
                tag: el.tagName,
                cls: el.className.substring(0,50),
                children: el.children.length
            };
            if (el.children.length > 0) {
                info.firstChild = walk(el.children[0], depth+1);
            }
            return info;
        };
        return JSON.stringify(walk(card, 0));
    })()""")
    print(f"\nCard structure:\n{card_structure}")

    await bm.cleanup()

asyncio.run(main())
