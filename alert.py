import requests
import credentials

def send_notif(message, chat_id='6774030856'):
    # if check_network():
        # chat_id = '6774030856' # Ann chat
        # chat_id = '-4246098511' # allen group chat
        bot_key = credentials.TELEGRAM_BOT_TOKEN
        
        send_message_url = f'https://api.telegram.org/bot{bot_key}/sendMessage?chat_id={chat_id}&text={message}'
        res = requests.post(send_message_url)

        if res.ok:
            print('Message sent successfully!')
        else:
            print('message not sent')
  