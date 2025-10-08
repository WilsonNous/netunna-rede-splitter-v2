from apscheduler.schedulers.background import BackgroundScheduler
import requests

scheduler = BackgroundScheduler()

def daily_job():
    try:
        print('🕓 Executando varredura diária...')
        requests.post('http://localhost:5000/api/process', json={'filename': 'auto'})
    except Exception as e:
        print(f'❌ Erro na execução automática: {e}')

scheduler.add_job(daily_job, 'cron', hour=3, minute=0)
scheduler.start()
