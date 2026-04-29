ssh -i ~/.ssh/finglobal-api-data-ingestion_key.pem azureuser@4.188.80.28
cd ~/finglobal_api_ingestion_service
sudo apt update

sudo docker compose run --rm api_ingestion python -m app.api_main

sudo docker compose exec db psql -U admin -d financial_db -c "SELECT COUNT(*) FROM company_master;"

sudo docker compose exec db psql -U admin -d financial_db -c "SELECT COUNT(*) FROM board;"