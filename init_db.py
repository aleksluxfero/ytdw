import os
import psycopg2

DB = os.environ.get('DATABASE_URL', 'postgresql://ytdlp:ytdlp@db:5432/ytdlp')
DDL = '''
CREATE TABLE IF NOT EXISTS downloads (
  id SERIAL PRIMARY KEY,
  url TEXT NOT NULL,
  format_id TEXT NOT NULL,
  file_id TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_downloads_url_fmt ON downloads(url, format_id);
'''

if __name__ == '__main__':
    with psycopg2.connect(DB) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
            conn.commit()
    print('DB ready')
