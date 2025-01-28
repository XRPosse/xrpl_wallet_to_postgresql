from datetime import datetime, timedelta, timezone
import json
import os
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from xrpl.clients import JsonRpcClient
from xrpl.wallet import Wallet
from xrpl.models.requests import AccountLines
from xrpl.models.requests import AccountTx
from xrpl.models.requests import NFTInfo
from xrpl.models.amounts import IssuedCurrencyAmount
from xrpl.ledger import get_latest_validated_ledger_sequence
import pytz

# XRPL Node URL (use a public testnet or mainnet node)
URL = "https://s1.ripple.com:51234"  # Example for Testnet, adjust for Mainnet
client = JsonRpcClient(URL)

# PostgreSQL Connection Details

DB_NAME = "gr"
DB_USER = "postgres"
DB_PASS = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"

def create_tables(cursor, conn, wallet_id):
    cm = f'''
    CREATE TABLE IF NOT EXISTS {wallet_id} (
        id INTEGER PRIMARY KEY,
        timestamp timestamp NOT NULL,
        tx_hash TEXT NOT NULL,
        action TEXT NOT NULL,
        sender_wallet TEXT NOT NULL,
        receiver_wallet TEXT,
        tkn_id TEXT,
        tkn_issuer TEXT,
        tkn_value NUMERIC,
        xrp_value NUMERIC,
        lp_tkn_id TEXT,
        lp_tkn_issuer TEXT,
        lp_tkn_value TEXT,
        nft_id TEXT,
        nft_sending_owner TEXT
    );
    '''
    cursor.execute(cm)
    conn.commit()

def get_full_transaction_history(wallet_address):
    transactions = []
    marker = None

    while True:
        response = client.request(AccountTx(
            account=wallet_address,
            ledger_index_min=-1,
            ledger_index_max=-1,
            limit=200,  # Adjust the limit as needed
            marker=marker
        ))

        transactions.extend(response.result['transactions'])

        # Check if there is a marker for the next page
        if 'marker' in response.result:
            marker = response.result['marker']
        else:
            break

    return transactions

def tx_to_files(tx):
    # print('tx meta:', tx['meta'])
    # os.mkdir(f'/home/rese/Documents/rese/xrplWalletTracker/test_tx/{str(isit)}')
    with open(f'/home/rese/Documents/rese/xrplWalletTracker/t/tx_meta.json', 'w') as json_file:
        json.dump(tx['meta'], json_file, indent=4)
        json_file.close()
    # print('tx tx_json:', tx['tx_json']) #tx['tx_json']['TransactionType'] == 'Payment'
    with open(f'/home/rese/Documents/rese/xrplWalletTracker/t/tx_json.json', 'w') as json_file:
        json.dump(tx['tx_json'], json_file, indent=4)
        json_file.close()
        # print('tx ledger_index:', tx['ledger_index'])
        # with open('/home/rese/Documents/rese/xrplWalletTracker/ledger_index.json', 'w') as json_file:
        #     json.dump(tx['ledger_index'], json_file, indent=4)
        #     json_file.close()
        # print('tx hash:', tx['hash'])
    # with open(f'/home/rese/Documents/rese/xrplWalletTracker/test_tx/{str(isit)}/hash.json', 'w') as json_file:
    #     json.dump(tx['hash'], json_file, indent=4)
    #     json_file.close()
        # print('tx ledger_hash:', tx['ledger_hash'])
        # with open('/home/rese/Documents/rese/xrplWalletTracker/ledger_hash.json', 'w') as json_file:
        #     json.dump(tx['ledger_hash'], json_file, indent=4)
        #     json_file.close()
        # print('tx close_time_iso:', tx['close_time_iso'])
    # with open(f'/home/rese/Documents/rese/xrplWalletTracker/test_tx/{str(isit)}/close_time_iso.json', 'w') as json_file:
    #     json.dump(tx['close_time_iso'], json_file, indent=4)
    #     json_file.close()
        # print('tx validated:', tx['validated'])
        # with open('/home/rese/Documents/rese/xrplWalletTracker/validated.json', 'w') as json_file:
        #     json.dump(tx['validated'], json_file, indent=4)
        #     json_file.close()

def process_transaction(tx, wallet_address, cursor, conn):
    if tx['meta']['TransactionResult'] == 'tesSUCCESS' and tx['tx_json']['TransactionType'] != 'NFTokenCreateOffer':
        tx_type = tx['tx_json']['TransactionType']
        from_wallet = tx['tx_json']['Account']
        tx_hash = tx['hash']
        timestamp = tx['close_time_iso'] # "2025-01-22T17:56:11Z"
        dtz = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
        dt = dtz.replace(tzinfo=pytz.UTC)
        id = int(dt.replace(tzinfo=timezone.utc).timestamp())

        # print('tx_type:', tx_type)
        if tx_type == 'Payment' and tx['meta']['delivered_amount'] != '1':
            isXRP = False
            to_wallet = tx['tx_json']['Destination']
            try:
                deliver_currency = tx['tx_json']['DeliverMax']['currency']
                deliver_currency_issuer = tx['tx_json']['DeliverMax']['issuer']
                amount_token_recieved = tx['meta']['delivered_amount']['value']
                amount_xrp_paid = tx['tx_json']['SendMax']
                action = 'token_purchase'
                isXRP = False
            except:
                isXRP = True

            if isXRP:
                try:
                    sold_currency = tx['tx_json']['SendMax']['currency']
                    sold_currencyIssuer = tx['tx_json']['SendMax']['issuer']
                    xrp_recieved = tx['meta']['delivered_amount']
                    sold_amount = tx['tx_json']['SendMax']['value']
                    action = 'token_sell'
                    isXRP = False
                except:
                    isXRP = True

            if isXRP:
                try:
                    if wallet_address == from_wallet:
                        action = 'token_payment'
                    else:
                        action = 'token_receive'
                    pr_currency = tx['meta']['delivered_amount']['currency']
                    pr_currency_issuer = tx['meta']['delivered_amount']['issuer']
                    pr_amount = tx['meta']['delivered_amount']['value']
                    isXRP = False
                except:
                    isXRP = True

            if isXRP:
                try:
                    if from_wallet == wallet_address:
                        action = 'xrp_payment'
                    else:
                        action = 'xrp_receive'
                    isXRP = True
                except Exception as e:
                    isXRP = False

            # print('action:', action)
            if isXRP:
                try:
                    delivered_amount = tx['meta']['delivered_amount']
                    q = "INSERT INTO %s (id, timestamp, tx_hash, action, tkn_id, xrp_value, sender_wallet, receiver_wallet) VALUES (%%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s)" % wallet_address
                    cursor.execute(q, (id, dt, tx_hash, action, 'xrp', delivered_amount, from_wallet, to_wallet))
                except Exception as e:
                    print('delivered_amount error:', e)
                    
            elif action == 'token_payment' or action == 'token_receive':
                try:
                    q = "INSERT INTO %s (id, timestamp, tx_hash, action, tkn_id, tkn_issuer, tkn_value, sender_wallet, receiver_wallet) VALUES (%%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s)" % wallet_address
                    cursor.execute(q, (id, dt, tx_hash, action, pr_currency, pr_currency_issuer, pr_amount, from_wallet, to_wallet))
                except Exception as e:
                    print('token_payment error:', e)
            elif action == 'token_purchase':
                try:
                    q = "INSERT INTO %s (id, timestamp, tx_hash, action, xrp_value, tkn_id, tkn_issuer, tkn_value, sender_wallet, receiver_wallet) VALUES (%%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s)" % wallet_address
                    cursor.execute(q, (id, dt, tx_hash, action, amount_xrp_paid, deliver_currency, deliver_currency_issuer, amount_token_recieved, from_wallet, to_wallet))
                except Exception as e:
                    print('token_purchase error:', e)
                # sell token
            elif action == 'token_sell':
                try:
                    q = "INSERT INTO %s (id, timestamp, tx_hash, action, tkn_id, tkn_issuer, tkn_value, xrp_value, sender_wallet, receiver_wallet) VALUES (%%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s)" % wallet_address
                    cursor.execute(q, (id, dt, tx_hash, action, sold_currency, sold_currencyIssuer, sold_amount, xrp_recieved, from_wallet, to_wallet))
                except Exception as e:
                    print('token_sell error:', e)
        
        elif tx_type == 'AMMWithdraw':
            try:
                tkn_id = tx['tx_json']['Amount']['currency']
                tkn_issuer = tx['tx_json']['Amount']['issuer']
                tkn_value = tx['tx_json']['Amount']['value']
                xrp_value = 0
                lp_tkn_id = tx['tx_json']['LPTokenIn']['currency']
                lp_tkn_issuer = tx['tx_json']['LPTokenIn']['issuer']
                lp_tkn_value = tx['tx_json']['LPTokenIn']['value']
                action = 'amm_ss_tkn_withdraw'
                isDbl = False
            except:
                isDbl = True

            if isDbl:
                try:
                    tkn_id = 'xrp'
                    tkn_issuer = ''
                    tkn_value = 0
                    xrp_value = tx['tx_json']['Amount']
                    lp_tkn_id = tx['tx_json']['LPTokenIn']['currency']
                    lp_tkn_issuer = tx['tx_json']['LPTokenIn']['issuer']
                    lp_tkn_value = tx['tx_json']['LPTokenIn']['value']
                    action = 'amm_ss_xrp_withdraw'
                    isDbl = False
                except:
                    isDbl = True

            if isDbl:
                try:
                    if tx['tx_json']['Asset']['currency'] == 'XRP':
                        tkn_id = tx['tx_json']['Asset2']['currency']
                        tkn_issuer = tx['tx_json']['Asset2']['issuer']
                    else:
                        tkn_id = tx['tx_json']['Asset']['currency']
                        tkn_issuer = tx['tx_json']['Asset']['issuer']

                    for node in tx['meta']['AffectedNodes']:
                        if 'ModifiedNode' in node and 'FinalFields' in node['ModifiedNode'] and 'LPTokenBalance' in node['ModifiedNode']['FinalFields'] and 'PreviousFields' in node['ModifiedNode'] and 'LPTokenBalance' in node['ModifiedNode']['PreviousFields']:
                            amm_token_node = node
                        if 'ModifiedNode' in node and 'FinalFields' in node['ModifiedNode'] and 'Balance' in node['ModifiedNode']['FinalFields'] and 'PreviousFields' in node['ModifiedNode'] and 'Balance' in node['ModifiedNode']['PreviousFields'] and 'currency' in node['ModifiedNode']['PreviousFields']['Balance'] and node['ModifiedNode']['PreviousFields']['Balance']['currency'] == tkn_id:
                            tkn_node = node

                    xrp_node = tx['meta']['AffectedNodes'][0]
                    lp_tkn_id = amm_token_node['ModifiedNode']['FinalFields']['LPTokenBalance']['currency']
                    lp_tkn_issuer = amm_token_node['ModifiedNode']['FinalFields']['LPTokenBalance']['issuer']
                    final_balance = amm_token_node['ModifiedNode']['FinalFields']['LPTokenBalance']['value']
                    prev_balance = amm_token_node['ModifiedNode']['PreviousFields']['LPTokenBalance']['value']
                    lp_tkn_value =  abs(float(final_balance) - float(prev_balance))
                    
                    tkn_final_balance = tkn_node['ModifiedNode']['FinalFields']['Balance']['value']
                    tkn_prev_balance = tkn_node['ModifiedNode']['PreviousFields']['Balance']['value']
                    tkn_value =  abs(float(tkn_final_balance) - float(tkn_prev_balance))

                    xrp_final_balance = xrp_node['ModifiedNode']['FinalFields']['Balance']
                    xrp_prev_balance = xrp_node['ModifiedNode']['PreviousFields']['Balance']
                    xrp_value =  abs(float(xrp_final_balance) - float(xrp_prev_balance))

                    action = 'amm_dbl_withdrawl'
                    isDbl = True
                except:
                    isDbl = False
            q = "INSERT INTO %s (id, timestamp, tx_hash, action, lp_tkn_id, lp_tkn_issuer, lp_tkn_value, xrp_value, tkn_id, tkn_issuer, tkn_value, sender_wallet) VALUES (%%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s)" % wallet_address
            cursor.execute(q, (id, dt, tx_hash, action, lp_tkn_id, lp_tkn_issuer, lp_tkn_value, xrp_value, tkn_id, tkn_issuer, tkn_value, from_wallet))
        
        elif tx_type == 'AMMDeposit':
            try:
                tkn_id = tx['tx_json']['Amount']['currency']
                tkn_issuer = tx['tx_json']['Amount']['issuer']
                tkn_value = tx['tx_json']['Amount']['value']
                xrp_value = 0
                for node in tx['meta']['AffectedNodes']:
                    if 'ModifiedNode' in node and 'FinalFields' in node['ModifiedNode'] and 'LPTokenBalance' in node['ModifiedNode']['FinalFields'] and 'PreviousFields' in node['ModifiedNode'] and 'LPTokenBalance' in node['ModifiedNode']['PreviousFields']:
                        amm_token_node = node
                lp_tkn_id = amm_token_node['ModifiedNode']['FinalFields']['LPTokenBalance']['currency']
                lp_tkn_issuer = amm_token_node['ModifiedNode']['FinalFields']['LPTokenBalance']['issuer']
                final_balance = amm_token_node['ModifiedNode']['FinalFields']['LPTokenBalance']['value']
                prev_balance = amm_token_node['ModifiedNode']['PreviousFields']['LPTokenBalance']['value']
                lp_tkn_value =  float(final_balance) - float(prev_balance)
                action = 'amm_ss_tkn_deposit'
                isDbl = False
            except Exception as e:
                print('amm_ss_tkn_deposit error', e)
                isDbl = True

            if isDbl:
                try:
                    tkn_id = 'xrp'
                    tkn_issuer = ''
                    tkn_value = 0
                    xrp_value = tx['tx_json']['Amount']
                    for node in tx['meta']['AffectedNodes']:
                        if 'ModifiedNode' in node and 'FinalFields' in node['ModifiedNode'] and 'LPTokenBalance' in node['ModifiedNode']['FinalFields'] and 'PreviousFields' in node['ModifiedNode'] and 'LPTokenBalance' in node['ModifiedNode']['PreviousFields']:
                            amm_token_node = node
                    lp_tkn_id = amm_token_node['ModifiedNode']['FinalFields']['LPTokenBalance']['currency']
                    lp_tkn_issuer = amm_token_node['ModifiedNode']['FinalFields']['LPTokenBalance']['issuer']
                    final_balance = amm_token_node['ModifiedNode']['FinalFields']['LPTokenBalance']['value']
                    prev_balance = amm_token_node['ModifiedNode']['PreviousFields']['LPTokenBalance']['value']
                    lp_tkn_value =  float(final_balance) - float(prev_balance)
                    action = 'amm_ss_xrp_deposit'
                    isDbl = False
                except Exception as e:
                    print('amm_ss_xrp_deposit error', e)
                    isDbl = True

            if isDbl:
                try:
                    tkn_id = tx['tx_json']['Amount2']['currency']
                    tkn_issuer = tx['tx_json']['Amount2']['issuer']
                    tkn_value = tx['tx_json']['Amount2']['value']
                    xrp_value = tx['tx_json']['Amount']
                    for node in tx['meta']['AffectedNodes']:
                        if 'ModifiedNode' in node and 'FinalFields' in node['ModifiedNode'] and 'LPTokenBalance' in node['ModifiedNode']['FinalFields'] and 'PreviousFields' in node['ModifiedNode'] and 'LPTokenBalance' in node['ModifiedNode']['PreviousFields']:
                            amm_token_node = node
                    lp_tkn_id = amm_token_node['ModifiedNode']['FinalFields']['LPTokenBalance']['currency']
                    lp_tkn_issuer = amm_token_node['ModifiedNode']['FinalFields']['LPTokenBalance']['issuer']
                    final_balance = amm_token_node['ModifiedNode']['FinalFields']['LPTokenBalance']['value']
                    prev_balance = amm_token_node['ModifiedNode']['PreviousFields']['LPTokenBalance']['value']
                    lp_tkn_value =  float(final_balance) - float(prev_balance)
                    action = 'amm_dbl_deposit'
                    isDbl = True
                except Exception as e:
                    print('amm_dbl_deposit error', e)
                    isDbl = False
            q ="INSERT INTO %s (id, timestamp, tx_hash, action, lp_tkn_id, lp_tkn_issuer, lp_tkn_value, xrp_value, tkn_id, tkn_issuer, tkn_value, sender_wallet) VALUES (%%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s)" % wallet_address
            cursor.execute(q, (id, dt, tx_hash, action, lp_tkn_id, lp_tkn_issuer, lp_tkn_value, xrp_value, tkn_id, tkn_issuer, tkn_value, from_wallet))
        
        elif tx_type == 'NFTokenAcceptOffer':
            if tx['tx_json']['Account'] != wallet_address and 'NFTokenBuyOffer' not in tx['tx_json']:
                xrp_value = 0
                from_wallet = wallet_address
                to_wallet = tx['tx_json']['Account']
                action = 'nft_sent'
                nft_id = tx['meta']['nftoken_id']
                for node in tx['meta']['AffectedNodes']:
                    if 'DeletedNode' in node and 'FinalFields' in node['DeletedNode'] and 'Owner' in node['DeletedNode']['FinalFields']:
                        nft_sending_owner = node['DeletedNode']['FinalFields']['Owner']
                        xrp_value = 0
            elif tx['tx_json']['Account'] == wallet_address and 'NFTokenBuyOffer' not in tx['tx_json']:
                xrp_value = 0
                to_wallet = wallet_address
                from_wallet = tx['tx_json']['Account']
                action = 'nft_recieved'
                nft_id = tx['meta']['nftoken_id']
                for node in tx['meta']['AffectedNodes']:
                    if 'DeletedNode' in node and 'FinalFields' in node['DeletedNode'] and 'Owner' in node['DeletedNode']['FinalFields']:
                        nft_sending_owner = node['DeletedNode']['FinalFields']['Owner']
                        xrp_value = 0
            else:
                for node in tx['meta']['AffectedNodes']:
                    if 'DeletedNode' in node and 'FinalFields' in node['DeletedNode'] and 'Owner' in node['DeletedNode']['FinalFields']:
                        nft_sending_owner = node['DeletedNode']['FinalFields']['Owner']
                    if 'ModifiedNode' in node and 'FinalFields' in node['ModifiedNode']  and 'Balance' in node['ModifiedNode']['FinalFields'] and 'PreviousFields' in node['ModifiedNode'] and 'Balance' in node['ModifiedNode']['PreviousFields']:
                        final_value = node['ModifiedNode']['FinalFields']['Balance']
                        pre_value = node['ModifiedNode']['PreviousFields']['Balance']
                        dif = float(final_value) - float(pre_value)
                        nft_id = tx['meta']['nftoken_id']
                        if dif > 0:
                            action = 'nft_sell'
                            xrp_value = dif
                            from_wallet = wallet_address
                            to_wallet = tx['tx_json']['Account']
                        else:
                            action = 'nft_purchase'
                            xrp_value = abs(dif)
                            from_wallet = tx['tx_json']['Account']
                            to_wallet = wallet_address
            q = "INSERT INTO %s (id, timestamp, tx_hash, action, nft_id, nft_sending_owner, xrp_value, sender_wallet, receiver_wallet) VALUES (%%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s)" % wallet_address
            cursor.execute(q, (id, dt, tx_hash, action, nft_id, nft_sending_owner, xrp_value, from_wallet, to_wallet))

        elif tx_type == 'TrustSet':
            tkn_id = tx['tx_json']['LimitAmount']['currency']
            tkn_issuer = tx['tx_json']['LimitAmount']['issuer']
            tkn_value = tx['tx_json']['LimitAmount']['value']
            if tkn_value == 0:
                action = 'trust_remove'
            else:
                action = 'trust_add'
            q = "INSERT INTO %s (id, timestamp, tx_hash, action, tkn_id, tkn_issuer, sender_wallet) VALUES (%%s, %%s, %%s, %%s, %%s, %%s, %%s)" % wallet_address
            cursor.execute(q, (id, dt, tx_hash, action, tkn_id, tkn_issuer, wallet_address))
        
def main(wallet_address):
    with psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_USER, host=DB_HOST, port=DB_PORT) as conn:
        with conn.cursor() as cursor:
            create_tables(cursor, conn, wallet_address)
            ftx = get_full_transaction_history(wallet_address)
            # response = client.request(AccountTx(account=wallet_address, ledger_index_min=-1, ledger_index_max=-1))
            for tx in ftx:
                # tx_to_files(tx)
                process_transaction(tx, wallet_address, cursor, conn)
            conn.commit()

if __name__ == "__main__":
    # wallet_address = input("Enter the XRPL wallet address: ")
    # rJf9D35rEzgsgQ9UwDbfYAPwvLRPsjKmV8
    # rUoqDqEnHWczSCMMQGfdu2D32qXVnpR2Fm
    wallet_address='rJf9D35rEzgsgQ9UwDbfYAPwvLRPsjKmV8'
    main(wallet_address)