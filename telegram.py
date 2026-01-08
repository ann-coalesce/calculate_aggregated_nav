import credentials
import requests

def send_notif(message, chat_id='-5039629904'):
    # chat_id = '6774030856' # Ann chat
    # chat_id = '-5039629904' # data pipeline alerts
    bot_key =  credentials.TELEGRAM_BOT_TOKEN # '7184236096:AAHlBC4MeqckU4x2B5R6U7Aie96eyMdCnpk'
    
    send_message_url = f'https://api.telegram.org/bot{bot_key}/sendMessage?chat_id={chat_id}&text={message}'
    res = requests.post(send_message_url)

    if res.ok:
        print('Message sent successfully!')
    else:
        print('message not sent')