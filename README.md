# Flexible Web Scraping Framework

![Built with uv](https://img.shields.io/badge/Built%20with-uv-purple)

A modular, configuration-driven web scraping framework designed for modern, dynamic websites. Built on top of `nodriver` for superior anti-bot detection avoidance, it utilizes Strategy and Factory patterns to decouple scraping logic from site-specific configurations.

## Quick Start

1.  **Install uv**:
    ```bash
    curl -LsSf https://astral.sh/uv/install.sh | sh
    ```

2.  **Run the Scraper**:
    Run the Google Maps example immediately. `uv` handles dependency management automatically.
        ```bash
        uv run python main.py --config config/google_maps.yaml --query "restaurants in NYC" --no-headless
        ```

## Key Features

*   **Strategy Pattern Architecture**: Decoupled logic for Pagination, Extraction, and Output allows for easy extension.
*   **Headless Automation**: Uses `nodriver` (Chrome-based) to handle complex JavaScript and anti-bot measures.
*   **Configuration-Driven**: Define new scrapers via YAML without writing code for standard use cases.
*   **Robust Pagination**: Native support for Infinite Scroll, "Next" buttons, and AJAX loading.
*   **Duplicate Detection**: Built-in mechanisms to prevent saving duplicate entries.

## Prerequisites

*   **Python**: 3.12+
*   **Package Manager**: [uv](https://docs.astral.sh/uv/)
