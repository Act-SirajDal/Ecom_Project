import json
from datetime import datetime
from pathlib import Path

DATE = datetime.now().strftime("%Y%m%d")
# DATE = '20241220'

# db_host = '172.27.131.60'
db_host = 'localhost'
db_user = 'root'
db_password = 'actowiz'
db_port = 3306

# Daily Runs....
fk_meesho_master_mapping_db = "fk_meesho_master_mapping"
sy_meesho_db = "sy_meesho"

# Weekly Runs....
fk_meesho_vertical_master_db = "fk_meesho_vertical_master"
sy_meesho_vertical_master_db = "sy_meesho_vertical_master"

# uncertainty files...
fk_meesho_mapping_db = "fk_meesho_mapping"


# Initialize Slack client with your bot token
# slack_bot_credentials_dict: dict = json.loads(Path("slack_bot_credentials.json").read_text(encoding="utf-8"))
# slack_bot_token: str = slack_bot_credentials_dict.get('slack_bot_token_dict', {}).get('slack_bot_token', 'N/A')  # Replace with your bot token
# daily_msgs_channel_id: str = slack_bot_credentials_dict.get('daily_msgs_channel_id_dict', {}).get('daily_msgs_channel_id', 'N/A')  # Replace with your bot token

# print(slack_bot_token)
# print(daily_msgs_channel_id)