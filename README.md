# Store Monitoring System V0

A backend service to monitor restaurant/store uptime/downtime, ingest large CSV datasets (~1.9M records), and generate weekly uptime reports within a minute.

Test using docker run -p 8000:8000   -v $(pwd)/reports:/app/data/reports   nxvtej7/store-monitoring:latest 

---

## Tech Stack

**Python** · **FastAPI** · **Postgre** · **SQLAlchemy** · **Docker**

---

## Stats

- **Data Ingestion:** ~150 sec (includes table creation + ingesting ~1.9M records)  
- **Report Generation:** ~40 sec  

---

## 🔗 Links

- [Output Report (Google Drive)](https://drive.google.com/file/d/1Rb67b9pZQrx79V8aJhybqTtJCoyWq1IG/view?pli=1)  
- [Demo Video (Loom)](https://www.loom.com/share/32a0ce70a3c8463bb94ebc64fa770b41?sid=e0ad25a1-5842-4845-956e-4e95dd74eeb6)

---

## Future Work

1. Offload report-generation logic from main API server to background workers (e.g., via AWS ECS + via messaging queues Redis/Kafka).
2. Use **DuckDB** for faster analytical queries; supports lightweight local file-based operations. and remove need for external database
3. Add a polling service to fetch status data automatically (removing dependency on manual CSV ingestion).
4. Seperate db for storing reports, cost-effective storage (e.g., S3 buckets).
5. Better logging using structured logger with file dumps for easier debugging.

---

## Endpoints

### 1. `POST /trigger_report`
- **Description:** Initiates report generation in the background.
- **Input:** None
- **Response:**
  ```json
  {
    "report_id": "86e11526-1075-4134-89b6-3d99176c1d20",
    "status": "Queued",
    "message": "Report generation started in background."
  }
  ```
### 2. `GET /get_report/{report_id}`
- **Description:** Checks the status of the report or returns the generated CSV.
- **Input:** report_id (UUID)
- **Response:**
   - If the report is still being generated:

  ```json
   {
      "status": "Running",
      "message": "Report is still being generated. Please try again later."
   }
  ```

   - If the report is ready:
      The CSV file is returned as a downloadable attachment


---

## Setup

0. **Docker**
   ```bash
   docker run -p 8000:8000   -v $(pwd)/reports:/app/data/reports   nxvtej7/store-monitoring:latest #wait ~260sec to complete ingestion and run api-server 
   ```

1. **Clone repository**  
   ```bash
   git clone https://github.com/nxvtej/navdeep_15_june_2025.git
   cd navdeep_15_june_2025
   ```

2. **Create a virtual environment**  
   ```bash
   python -m venv venv
   source venv/bin/activate #linux
   venv/Scripts/activate #windows
   ```

3. **Install dependencies**  
   ```bash
   pip install -r req.txt
   ```

4. **Start Postgre with Docker**  
   ```bash
   docker compose up -d
   ```

5. **Create a `.env` file** in the root directory  
   ```env
   DATABASE_URL=postgresql://user:password@localhost:5432/store_monitoring_db
   ```

6. **Create a `data/` folder in the root directory** and place all CSV files inside it.

7. **Run the data ingestion script** (uses 6 threads — close other intensive tasks)  
   ```bash
   python -m business.ingest_data
   ```

8. **Start API server**  
   ```bash
   uvicorn app.main:app --reload
   ```

9. **Trigger a report generation**  
   ```bash
   curl --location --request POST 'http://127.0.0.1:8000/trigger_report'
   ```

10. **Check report status**  
    ```bash
    curl --location --globoff 'http://127.0.0.1:8000/get_report/{report_id}'
    ```

11. **Generated reports will be saved under:**  
    ```
    /data/reports
    ```

---

