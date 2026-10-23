# 02739452 — Media Trends Pipeline (Movies · Series · Anime)

ระบบรวบรวมข้อมูลสื่อบันเทิงที่ยอดนิยมในปีปัจจุบันจาก 3 แหล่ง มารวมเป็นชุดข้อมูลเดียวด้วย **Apache Airflow DAG**
เก็บใน **PostgreSQL** และแสดงผลบน **Streamlit dashboard** เพื่อเปรียบเทียบคะแนน ความนิยม และสัดส่วนประเภท (Genre)

## หัวข้อและแหล่งข้อมูล

| แหล่ง | วิธีดึง | ข้อมูล |
|---|---|---|
| TMDB (themoviedb.org) | REST API (ต้องมี API key) | หนังและซีรีส์ยอดนิยมที่ออกในปีปัจจุบัน (อย่างละ ~50) |
| Jikan (jikan.moe / MyAnimeList) | REST API | อนิเมะที่ออกฉายในปีปัจจุบัน เรียงตามจำนวนสมาชิก (~100) |
| TVMaze (tvmaze.com) | REST API + **web scraping** | ซีรีส์ที่ออกอากาศช่วง 7 วันล่าสุด เรียงตาม popularity; scrape หน้าเว็บเพื่อเอาจำนวนโหวต (votes) |

- Web scraping ใช้ `requests` + `BeautifulSoup` (ไม่ใช้ Selenium) ตรวจ `robots.txt` ก่อนทุกหน้า และหน่วงเวลา 1 วินาที/หน้า
- ไม่เก็บข้อมูลส่วนบุคคล (เก็บเฉพาะชื่อเรื่อง คะแนน ประเภท วันที่ และจำนวนผู้ติดตามรวม)

## สถาปัตยกรรม

```
extract (TaskGroup, รันขนาน)        transform            quality_gate            load / quarantine     log_run
 ├─ tmdb (API)                                             ├─ pass → load_to_postgres (UPSERT) ─┐
 ├─ jikan (API)             ──►  clean + genre map  ──►    │                                    ├─► log_run ─► dashboard
 └─ tvmaze_api → tvmaze_scrape   + date normalise          └─ fail → quarantine (รายงาน JSON) ──┘
                                 + cross-source dedupe
```

- **ระดับ 1:** extract → transform → load → PostgreSQL → เว็บแสดงตารางและกราฟ
- **ระดับ 2:** อย่างน้อย 2 แหล่งรันขนานใน TaskGroup, schema เดียวกัน, UPSERT (`ON CONFLICT`) รันซ้ำแล้วจำนวนแถวไม่เพิ่ม, เว็บมี KPI และตัวกรอง
- **ระดับ 3:** 3 แหล่ง, `transform` ใช้ `trigger_rule=all_done` จึงรวมข้อมูลจากแหล่งที่เหลือได้เมื่อบางแหล่งล่ม, ตัดรายการซ้ำข้ามแหล่ง (ชื่อที่ normalize แล้ว + ปี), `quality_gate` (BranchPython) แตกเส้นทางไป `load_to_postgres` หรือ `quarantine`
- **โบนัส:** แท็บ *Pipeline status* บนเว็บ อ่านจากตาราง `pipeline_runs`

## ตาราง PostgreSQL (`db/schema.sql`)

`media`, `ratings`, `genres`, `media_genres`, `pipeline_runs` และ view `v_media`, `v_media_genre`

## วิธีรัน

```bash
cp .env.example .env        # แล้วใส่ TMDB_API_KEY (ฟรีที่ themoviedb.org/settings/api) และรหัสผ่าน
docker compose up --build
```

1. เปิด Airflow ที่ <http://localhost:8080> (user/password ตามใน `.env`)
2. กด Trigger DAG `media_trends_pipeline`
3. เมื่อ DAG สำเร็จ เปิด dashboard ที่ <http://localhost:8501>

รันซ้ำได้โดยจำนวนแถวใน `media` ไม่เพิ่ม (UPSERT) ตรวจได้ด้วย
`docker compose exec postgres psql -U $POSTGRES_USER -d media -c "select count(*) from media"`

## โครงสร้างโฟลเดอร์

```
dags/   media_trends_dag.py + media_pipeline/ (extract, transform, quality, load)
web/    app.py (Streamlit) + Dockerfile + requirements.txt
db/     schema.sql, init.sh
docker-compose.yml, Dockerfile, requirements.txt, .env.example
```
