import os, re, time, requests
from bs4 import BeautifulSoup
from urllib.parse import quote

HEADERS = {'User-Agent': 'MovieDataIntegrationStudentProject/1.0 (educational research; contact: configure-your-email)'}

def get_json(url, params):
    r=requests.get(url,params=params,headers=HEADERS,timeout=25)
    r.raise_for_status()
    return r.json()


def tmdb():
    key = os.environ.get('TMDB_API_KEY', '')
    if not key:
        raise ValueError('TMDB_API_KEY missing')

    out = []

    for year in range(2020, 2026):
        for page in range(1, 6):
            data = get_json(
                'https://api.themoviedb.org/3/discover/movie',
                {
                    'api_key': key,
                    'primary_release_year': year,
                    'sort_by': 'popularity.desc',
                    'page': page
                }
            )

            for m in data.get('results', []):
                out.append({
                    'title': m['title'],
                    'release_year': year,
                    'tmdb_rating': m.get('vote_average'),
                    'source': 'tmdb'
                })

            time.sleep(0.2)

    return out


def omdb(seed):
    key=os.environ.get('OMDB_API_KEY','')
    if not key: raise ValueError('OMDB_API_KEY missing')
    out=[]
    for m in seed[::max(1, len(seed)//80)][:80]:
        data=get_json('https://www.omdbapi.com/',{'apikey':key,'t':m['title'],'y':m['release_year']})
        if data.get('Response')=='True':
            runtime=data.get('Runtime','')
            out.append({'title':data['Title'],'release_year':int(data['Year'][:4]),'imdb_id':data.get('imdbID'),
              'imdb_rating':float(data['imdbRating']) if data.get('imdbRating') not in ('N/A',None) else None,
              'runtime_minutes':int(runtime.split()[0]) if re.match(r'^\d+ min$',runtime) else None,'genre':data.get('Genre'),'source':'omdb'})
        time.sleep(.12)
    return out




def wikipedia():
    from urllib.parse import urlparse

    out = []
    seen = set()

    for year in range(2020, 2026):
        url = f'https://en.wikipedia.org/wiki/List_of_American_films_of_{year}'

        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        # Wikipedia ใช้ตาราง wikitable สำหรับรายชื่อภาพยนตร์
        for table in soup.select('table.wikitable'):
            header_row = table.find('tr')
            if not header_row:
                continue

            headers = [
                re.sub(r'\s+', ' ', th.get_text(' ', strip=True)).lower()
                for th in header_row.find_all(['th', 'td'], recursive=False)
            ]

            # ตารางภาพยนตร์ต้องมีคอลัมน์ Title และข้อมูลเกี่ยวกับภาพยนตร์
            if 'title' not in headers:
                continue

            if not any(
                h in headers
                for h in ('production company', 'production companies',
                          'cast and crew', 'director', 'opening')
            ):
                continue

            title_index = headers.index('title')

            for tr in table.find_all('tr')[1:]:
                cells = tr.find_all(['th', 'td'], recursive=False)

                if len(cells) <= title_index:
                    continue

                cell = cells[title_index]

                # เลือกชื่อที่เชื่อมไปยังบทความ Wikipedia
                link = cell.find('a', href=re.compile(r'^/wiki/'))

                if link:
                    title = link.get_text(' ', strip=True)
                else:
                    title = cell.get_text(' ', strip=True)

                title = re.sub(r'\[\d+\]', '', title)
                title = re.sub(r'\s+', ' ', title).strip()

                if not title or len(title) < 2:
                    continue

                if title.lower() in ('title', 'nan', 'none'):
                    continue

                if re.fullmatch(r'\d+', title):
                    continue

                key = (title.casefold(), year)

                if key in seen:
                    continue

                seen.add(key)

                out.append({
                    'title': title,
                    'release_year': year,
                    'source': 'wikipedia'
                })

        time.sleep(0.5)

    return out


def normalize(items):
    merged={}
    for item in items:
        title=re.sub(r'\s+',' ',item.get('title','')).strip()
        year=item.get('release_year')
        if not title or not isinstance(year,int) or not 2020<=year<=2025:continue
        key=re.sub(r'[^a-z0-9]','',title.casefold())+':'+str(year)
        if not key.split(':')[0]:continue
        row=merged.setdefault(key,{'movie_key':key,'title':title,'release_year':year,'imdb_id':None,'genre':None,'tmdb_rating':None,'imdb_rating':None,'runtime_minutes':None,'sources':[],'duplicate_count':0})
        if item['source'] in row['sources']:
            row['duplicate_count'] += 1
        for field in ('imdb_id','genre','tmdb_rating','imdb_rating','runtime_minutes'):
            if item.get(field) is not None:row[field]=item[field]
        if item['source'] not in row['sources']:row['sources'].append(item['source'])
    return list(merged.values())
