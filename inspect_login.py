"""Inspect Instagram's current login page DOM to find correct selectors."""

import json
from playwright.sync_api import sync_playwright


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            locale="en-US",
        )
        page = ctx.new_page()
        page.goto("https://www.instagram.com/accounts/login/", wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle", timeout=15000)

        # Dump all input elements + their attributes
        inputs = page.evaluate("""
            () => Array.from(document.querySelectorAll('input')).map(el => ({
                tag: el.tagName,
                type: el.type,
                name: el.name,
                id: el.id,
                placeholder: el.placeholder,
                ariaLabel: el.getAttribute('aria-label'),
                autocomplete: el.autocomplete,
                className: el.className.substring(0, 80),
                outerHTML: el.outerHTML.substring(0, 300),
            }))
        """)
        print("\n=== INPUTS ===")
        print(json.dumps(inputs, indent=2))

        # Dump all buttons
        buttons = page.evaluate("""
            () => Array.from(document.querySelectorAll('button')).map(el => ({
                type: el.type,
                text: el.innerText.trim().substring(0, 60),
                ariaLabel: el.getAttribute('aria-label'),
                disabled: el.disabled,
                role: el.getAttribute('role'),
                outerHTML: el.outerHTML.substring(0, 200),
            }))
        """)
        print("\n=== BUTTONS ===")
        print(json.dumps(buttons, indent=2))

        # Dump form
        forms = page.evaluate("""
            () => Array.from(document.querySelectorAll('form')).map(el => ({
                action: el.action,
                method: el.method,
                id: el.id,
                className: el.className.substring(0, 80),
            }))
        """)
        print("\n=== FORMS ===")
        print(json.dumps(forms, indent=2))

        browser.close()


if __name__ == "__main__":
    main()
