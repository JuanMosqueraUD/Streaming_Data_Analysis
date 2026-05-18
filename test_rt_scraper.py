#!/usr/bin/env python3
"""
Test mejorado para debuggear el RT scraper.
Inspecciona estructura HTML y extracción de datos.
"""

import sys
import re
from pathlib import Path
from bs4 import BeautifulSoup
import requests

# Add airflow/utils to path
sys.path.insert(0, str(Path(__file__).parent / "airflow" / "utils"))

from rt_scraper import scrape_movies_at_home, _clean_title, RT_BASE_URL


def test_html_structure():
    """Descarga la página y muestra estructura de los items"""
    print("=" * 80)
    print("INSPECCIONANDO ESTRUCTURA HTML")
    print("=" * 80)
    
    url = "https://www.rottentomatoes.com/browse/movies_at_home"
    try:
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.content, "html.parser")
        
        # Buscar anchors con /m/ hrefs
        anchors = soup.find_all("a", href=re.compile(r"^/m/"))[:3]  # Solo primeros 3
        
        for idx, a in enumerate(anchors, 1):
            print(f"\n--- MOVIE {idx} ---")
            print(f"href: {a.get('href')}")
            
            # Mostrar HTML del anchor (primeros 500 chars)
            html_str = str(a)[:500]
            print(f"HTML: {html_str}...")
            
            # Mostrar spans directos
            spans = a.find_all("span", recursive=False)
            print(f"\nSpans directos: {len(spans)}")
            for i, span in enumerate(spans):
                text = span.get_text(strip=True)[:80]
                classes = span.get("class", [])
                data_qa = span.get("data-qa", "")
                print(f"  [{i}] text={text!r} | classes={classes} | data-qa={data_qa!r}")
            
            # Mostrar ALL text (recursivo)
            full_text = a.get_text(strip=True)[:150]
            print(f"\nTexto completo (get_text): {full_text!r}")
            
    except Exception as e:
        print(f"ERROR descargando: {e}")


def test_scraper_output():
    """Ejecuta el scraper y muestra los resultados"""
    print("\n\n" + "=" * 80)
    print("EJECUTANDO SCRAPER")
    print("=" * 80 + "\n")
    
    try:
        movies = scrape_movies_at_home()
        print(f"Total películas encontradas: {len(movies)}\n")
        
        # Mostrar primeras 10
        for i, m in enumerate(movies[:10], 1):
            title = m.get("title", "")
            critics = m.get("critics_score")
            audience = m.get("audience_score")
            date = m.get("release_date")
            
            print(f"{i:2}. {title!r}")
            print(f"    critics={critics} | audience={audience} | date={date!r}")
            
            # Detectar problemas
            if "Streaming" in title:
                print(f"    ⚠️  PROBLEMA: 'Streaming' aún en título")
            if "Link to" in title:
                print(f"    ⚠️  PROBLEMA: 'Link to' aún en título")
            if critics is None:
                print(f"    ⚠️  PROBLEMA: critics_score es None")
            if audience is None:
                print(f"    ⚠️  PROBLEMA: audience_score es None")
        
        # Estadísticas
        print(f"\n--- ESTADÍSTICAS ---")
        has_critics = sum(1 for m in movies if m.get("critics_score"))
        has_audience = sum(1 for m in movies if m.get("audience_score"))
        has_date = sum(1 for m in movies if m.get("release_date"))
        has_streaming = sum(1 for m in movies if "Streaming" in m.get("title", ""))
        has_link_to = sum(1 for m in movies if "Link to" in m.get("title", ""))
        
        print(f"Películas con critics_score:   {has_critics}/{len(movies)}")
        print(f"Películas con audience_score:  {has_audience}/{len(movies)}")
        print(f"Películas con release_date:    {has_date}/{len(movies)}")
        print(f"Películas con 'Streaming':     {has_streaming}/{len(movies)} ⚠️ ")
        print(f"Películas con 'Link to':       {has_link_to}/{len(movies)} ⚠️ ")
        
    except Exception as e:
        import traceback
        print(f"ERROR en scraper: {e}")
        traceback.print_exc()


def test_clean_title():
    """Prueba la función _clean_title"""
    print("\n\n" + "=" * 80)
    print("PROBANDO _clean_title()")
    print("=" * 80 + "\n")
    
    test_cases = [
        "Project Hail MaryStreaming",
        "ObsessionLink to Obsession",
        "The Sheep DetectivesLink to The Sheep Detectives",
        "Send HelpStreaming",
        "94% 95% My Title Streaming May 12, 2026",
    ]
    
    for original in test_cases:
        cleaned = _clean_title(original)
        print(f"Original: {original!r}")
        print(f"Cleaned:  {cleaned!r}")
        print()


if __name__ == "__main__":
    test_clean_title()
    test_html_structure()
    test_scraper_output()
