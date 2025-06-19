# kenya_external_debt
# Kenya's External Debt Analysis (2010–2024)

## **How to Run**
1. Install dependencies: `pip install -r requirements.txt`  
2. Set up PostgreSQL and run `schema.sql`  
3. Run ETL: `python etl_pipeline.py`  
4. Import Grafana dashboard using the JSON export  

## **Reflection**
- **Insights:** Kenya's debt has steadily increased since 2010, with spikes in [years].  
- **Challenges:** Handling missing World Bank data for recent years required interpolation.  
- **Learnings:** API rate limits and efficient PostgreSQL bulk inserts were key takeaways.   
