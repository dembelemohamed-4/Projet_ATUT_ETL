from bs4 import BeautifulSoup
from urllib.parse import urljoin
from minio import Minio
import requests
import pandas as pd

BASE_URL = 'https://books.toscrape.com'
# PATH = "/home/username/projet-atut/data/scrap.csv"
PATH =  "/opt/airflow/data/raw_data/scraping.csv"

# EXTRACTION
# Cette fonction permet de déterminer le nombre de pages du site web
def get_page_count() -> int:
  """Get the number of pages in the website"""
  
  response = requests.get(BASE_URL)
  if response.ok:
    content = response.text
    soup = BeautifulSoup(content, "html.parser")
    pager = soup.find('li', class_="current").text.strip()
    # pager retourne une chaine de caractère considéré comme une liste.
    # Donc on applique split sur cette dernière pour l'a coupe
    # sur of et on recupere le dernier élément.
    number_of_page = int(pager.split("of")[-1].strip())

  return number_of_page

# Recupère des infos sur un livre à partir d'une page
def get_books_infos(page) -> list:
   """Get books links from given page"""

   books_data = []
   CATALOG_URL = f'{BASE_URL}/catalogue'
   pagination_url = f'{CATALOG_URL}/page-{page}.html'

   response = requests.get(pagination_url)
   if response.ok:
    content = response.text
    soup = BeautifulSoup(content, "html.parser")
    articles = soup.find_all('article', class_="product_pod")

    for book in articles:
      title = book.h3.a.get("title")
      rating = book.p.get("class")[1]
      availability = book.find("p", class_="instock availability").text.strip()
      link = f'{CATALOG_URL}/{book.h3.a.get("href")}'
      price = book.find("p", class_="price_color").text.strip("Â£")
      image_link = book.a.img.get("src")
      absolute_link = urljoin(BASE_URL, image_link)

      book_html = requests.get(link)
      book_soup = BeautifulSoup(book_html.text, "html.parser")
      category_html = book_soup.select("ul.breadcrumb li a")[2]
      category = category_html.text.strip() if category_html else ""
      description_html = book_soup.find("div", id="product_description")
      description = description_html.find_next_sibling().text.strip() if description_html else "Pas de description"

      books_data.append({
        "title":title,
        "price":float(price),
        "rating":rating,
        "image_link":absolute_link,
        "availability":availability,
        "category":category,
        "link":link,
        "description":description
      })
      

    return books_data

# Parcours toutes les pages du site web
def get_all_books_details():
  """Get all books infos for each page of the webite"""

  number_of_page = get_page_count()
  books_links = []

  for page in range(1, number_of_page + 1):
    print(f"Scraping en cours: {page}/{number_of_page}")
    books_links.extend(get_books_infos(page))
  
  return books_links

def main_scraping():
  books = get_all_books_details()
  df = pd.DataFrame(books)

  # why sauvégarder en cvs
  # Pour éviter de rescraper le site lorsqu'on aurait besoin de recuperer les données
  df.to_csv(PATH, index=False)
  print(f"Scraping reussi: {len(books)} livres recuperés ")


# Cette fonction envoie le fichier vers mon minio
def upload_minio():
  """upload the files in minio"""
  
  MINIO_CLIENT  = Minio(
  "minio:9000",
  access_key="user",
  secret_key="password",
  secure=False
)
  
  bucket_name = "books"
  if not MINIO_CLIENT.bucket_exists(bucket_name):
    MINIO_CLIENT.make_bucket(bucket_name)

  MINIO_CLIENT.fput_object(bucket_name, "raw_data/scrap.csv", PATH)

def scrap_and_upload():
  main_scraping()
  upload_minio()