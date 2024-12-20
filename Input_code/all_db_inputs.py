from configs import *
from table_def import *
from slack_bot_functions import send_slack_message

# Initialize Slack client with your bot token
slack_bot_credentials_dict: dict = json.loads(Path("../slack_bot_credentials.json").read_text(encoding="utf-8"))
slack_bot_token: str = slack_bot_credentials_dict.get('slack_bot_token_dict', {}).get('slack_bot_token',
                                                                                      'N/A')  # Replace with your bot token
daily_msgs_channel_id: str = slack_bot_credentials_dict.get('daily_msgs_channel_id_dict', {}).get(
    'daily_msgs_channel_id', 'N/A')  # Replace with your bot token


def run_all_db_inputs():
    today_date = DATE
    send_slack_message(slack_bot_token=slack_bot_token, channel_id=daily_msgs_channel_id,
                       message=f'Started running all_db_inputs.py for: {today_date}')

    # Get the current day of the week (0=Monday, 1=Tuesday, 2=Wednesday, ... , 6=Sunday)
    current_day = datetime.now().weekday()

    list_of_db = [fk_meesho_master_mapping_db, fk_meesho_vertical_master_db, sy_meesho_db,sy_meesho_vertical_master_db]

    print('Inserting all data...')
    send_slack_message(slack_bot_token=slack_bot_token, channel_id=daily_msgs_channel_id,
                       message='Inserting all data...')

    for db in list_of_db:
        if db == "fk_meesho_master_mapping":
            try:
                print(f"Process Running for : {fk_meesho_master_mapping_db}")
                send_slack_message(slack_bot_token=slack_bot_token, channel_id=daily_msgs_channel_id,
                                   message=f'Process Running for : {db}')
                # create_table_fk_meesho_master(db)
            except Exception as e:
                print(f"Error while creating table in {db}, error: {e}")
                send_slack_message(slack_bot_token=slack_bot_token, channel_id=daily_msgs_channel_id,
                                   message=f'Error while creating table in : {db}')
        elif db == "sy_meesho":
            try:
                print(f"Process Running for : {sy_meesho_db}")
                send_slack_message(slack_bot_token=slack_bot_token, channel_id=daily_msgs_channel_id,
                                   message=f'Process Running for : {db}')
                # create_table_sy_meesho(db)
            except Exception as e:
                print(f"Error while creating table in {db}, error: {e}")
                send_slack_message(slack_bot_token=slack_bot_token, channel_id=daily_msgs_channel_id,
                                   message=f'Error while creating table in : {db}')
        elif db == "fk_meesho_vertical_master":
            if current_day == 2:
                try:
                    print(f"Process Running for : {db}")
                    send_slack_message(slack_bot_token=slack_bot_token, channel_id=daily_msgs_channel_id,
                                       message=f'Process Running for : {db}')
                    # create_table_fk_meesho_vertical_master(fk_meesho_vertical_master_db)
                except Exception as e:
                    print(f"Error while creating table in {db}, error: {e}")
                    send_slack_message(slack_bot_token=slack_bot_token, channel_id=daily_msgs_channel_id,
                                       message=f'Error while creating table in : {db}')
            else:
                print(f"Today is not Wednesday so Vertical not require for {db}")
                send_slack_message(slack_bot_token=slack_bot_token, channel_id=daily_msgs_channel_id,
                                   message=f"Today is not Wednesday so Vertical not require for {db}")
        elif db == "sy_meesho_vertical_master":
            if current_day == 2:
                try:
                    print(f"Process Running for : {db}")
                    send_slack_message(slack_bot_token=slack_bot_token, channel_id=daily_msgs_channel_id,
                                       message=f'Process Running for : {db}')
                    # create_table_sy_meesho_vertical_master(sy_meesho_vertical_master_db)
                except Exception as e:
                    print(f"Error while creating table in {db}, error: {e}")
                    send_slack_message(slack_bot_token=slack_bot_token, channel_id=daily_msgs_channel_id,
                                       message=f'"Error while creating table in : {db}')
            else:
                print(f"Today is not Wednesday so Vertical not require for {db}")
                send_slack_message(slack_bot_token=slack_bot_token, channel_id=daily_msgs_channel_id,
                                   message=f"Today is not Wednesday so Vertical not require for {db}")
        # elif db == "fk_meesho_mapping":
        #     try:
        #         print("Process Running for : ", fk_meesho_mapping_db)
        #         send_slack_message(slack_bot_token=slack_bot_token, channel_id=daily_msgs_channel_id,
        #                            message=f'Process Running for : {fk_meesho_mapping_db}')
        #         create_table_fk_meesho_mapping(fk_meesho_mapping_db)
        #     except Exception as e:
        #         print(f"Error while creating table in {fk_meesho_mapping_db}, error: {e}")
        #         send_slack_message(slack_bot_token=slack_bot_token, channel_id=daily_msgs_channel_id,
        #                            message=f"Error while creating table in {fk_meesho_mapping_db}, error: {e}")

        else:
            print("Please check Database...")
            send_slack_message(slack_bot_token=slack_bot_token, channel_id=daily_msgs_channel_id,
                               message=f'Please check Database.....')


if __name__ == '__main__':
    run_all_db_inputs()
