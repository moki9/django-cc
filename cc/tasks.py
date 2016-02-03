from __future__ import absolute_import
from socket import error as socket_error
from decimal import Decimal
from collections import defaultdict
from httplib import CannotSendRequest

from celery import shared_task
#from celery.utils.log import get_task_logger
from bitcoinrpc.authproxy import AuthServiceProxy

from django.db import transaction

from .models import (Wallet, Currency, Transaction, Address,
                       WithdrawTransaction, Operation)
from . import settings
from .signals import post_deposite

#logger = get_task_logger(__name__)


@shared_task(throws=(socket_error,))
@transaction.atomic
def query_transactions(ticker=None):
#   logger.info("Execute Query Transactions")
    if not ticker:
        #logger.warning("No ticker found. Performing Currency lookup.")
        for c in Currency.objects.all():
            #logger.info("Deffered Query Transactions: {}".format(c.ticker))
            query_transactions.delay(c.ticker)
        return

#    logger.info("Ticker found: {}".format(ticker))
    currency = Currency.objects.select_for_update().get(ticker=ticker)
    coin = AuthServiceProxy(currency.api_url)
#    logger.info("RPC Call to {}".format(currency.api_url))
    current_block = coin.getblockcount()
#    logger.info("Current Block: {}".format(current_block))
    processed_transactions = []

    block_hash = coin.getblockhash(currency.last_block)
#    logger.info("Block Hash: {}".format(block_hash))
    blocklist = coin.listsinceblock(block_hash)
#    logger.info("Block List since {0} : {1}".format(block_hash, blocklist))
    transactions = blocklist['transactions']

#    logger.info("Transactions: {}".format(transactions))
    for tx in transactions:
        if tx['txid'] in processed_transactions:
            continue

        if tx['category'] not in ('receive', 'generate', 'immature'):
            continue

#        logger.info("Processing Transactions: {}".format(tx))
        process_deposite_transaction(tx, ticker)
        processed_transactions.append(tx['txid'])

    currency.last_block = current_block
#    log.info("Latest block: {}".format(current_block))
    currency.save()

    for tx in Transaction.objects.filter(processed=False, currency=currency):
#        logger.info("Querying Tx: {0} -> {1}".format(ticker, tx.txid))
        query_transaction(ticker, tx.txid)


@transaction.atomic
def process_deposite_transaction(txdict, ticker):
#    logger.info("Execute Process Deposit Transaction")
    if txdict['category'] not in ('receive', 'generate', 'immature'):
#        logger.warning("Invalid TX: {}".format(txdict))
        return

    try:
        address = Address.objects.select_for_update().get(address=txdict['address'])
 #       logger.info("Got Address object for: {}".format(txdict['address']))
    except Address.DoesNotExist:
 #       logger.warning("No Local Address found for: {}".format(txdict['address']))
        return

    currency = Currency.objects.get(ticker=ticker)

    try:
 #       logger.info("Local Wallet lookup")
        wallet = Wallet.objects.select_for_update().get(addresses=address)
    except Wallet.DoesNotExist:
 #       logger.warning("No Local Wallet found, creating unknown wallet..")
        wallet, created = Wallet.objects.select_for_update().get_or_create(
            currency=currency,
            label='_unknown_wallet'
        )
        address.wallet = wallet
        address.save()

    tx, created = Transaction.objects.select_for_update().get_or_create(txid=txdict['txid'], address=txdict['address'], currency=currency)

    if tx.processed:
 #       logger.info("Transaction processed: {}".format(txdict['txid']))
        return

    if created:
 #       logger.info("Newly created TX: {}".format(txdict['txid']))
        if txdict['confirmations'] >= settings.CC_CONFIRMATIONS and txdict['category'] != 'immature':
 #           logger.info("TX Confirmed: {}".format(txdict['txid']))
            Operation.objects.create(
                wallet=wallet,
                balance=txdict['amount'],
                description='Deposite',
                reason=tx
            )
            wallet.balance += txdict['amount']
            wallet.save()
 #           logger.info("Updated Wallet:{0} Balance: {1}".format(wallet.label, wallet.balance))
            tx.processed = True
        else:
 #           logger.info("TX Unconfirmed: {}".format(txdict['txid']))
            Operation.objects.create(
                wallet=wallet,
                unconfirmed=txdict['amount'],
                description='Unconfirmed',
                reason=tx
            )
            wallet.unconfirmed += txdict['amount']
            wallet.save()

    else:
#        logger.info("Previously Created TX: {}".format(txdict['txid']))
        if txdict['confirmations'] >= settings.CC_CONFIRMATIONS and txdict['category'] != 'immature':
#            logger.info("TX Confirmed: {}".format(txdict['txid']))
            Operation.objects.create(
                wallet=wallet,
                unconfirmed=-txdict['amount'],
                balance=txdict['amount'],
                description='Confirmed',
                reason=tx
            )
            wallet.unconfirmed -= txdict['amount']
            wallet.balance += txdict['amount']
#            logger.info("Updated Wallet:{0} Balance: {1}".format(wallet.label, wallet.balance))
            wallet.save()
            tx.processed = True

    post_deposite.send(sender=process_deposite_transaction, instance=wallet)
    tx.save()
#    logger.info("Transaction saved: {0} ".format(tx))


@shared_task(throws=(socket_error,))
@transaction.atomic
def query_transaction(ticker, txid):
#    logger.info("Execute Query Transaction")
    currency = Currency.objects.select_for_update().get(ticker=ticker)
    coin = AuthServiceProxy(currency.api_url)
    for txdict in normalise_txifno(coin.gettransaction(txid)):
#        logger.info("Process deposit TX: {}".format(txdict))
        process_deposite_transaction(txdict, ticker)


def normalise_txifno(data):
    arr = []
    for t in data['details']:
        t['confirmations'] = data['confirmations']
        t['txid'] = data['txid']
        t['timereceived'] = data['timereceived']
        t['time'] = data['time']
        arr.append(t)
    return arr


@shared_task()
def refill_addresses_queue():
    for currency in Currency.objects.all():
        coin = AuthServiceProxy(currency.api_url)
        count = Address.objects.filter(currency=currency, active=True, wallet=None).count()

        if count < settings.CC_ADDRESS_QUEUE:
            for i in xrange(count, settings.CC_ADDRESS_QUEUE):
                try:
                    Address.objects.create(address=coin.getnewaddress(settings.CC_ACCOUNT), currency=currency)
                except (socket_error, CannotSendRequest) :
                    pass


@shared_task(throws=(socket_error,))
@transaction.atomic
def process_withdraw_transactions(ticker=None):
#    logger.info("Execute Process Withdraw Transaction")
    if not ticker:
#        logger.warning("No ticker found. Performing Currency lookup.")
        for c in Currency.objects.all():
#            logger.info("Deffered Process Withdraw Transaction: {}".format(c.ticker))
            process_withdraw_transactions.delay(c.ticker)
        return

    currency = Currency.objects.select_for_update().get(ticker=ticker)
    coin = AuthServiceProxy(currency.api_url)

#    logger.info("Withdraw Transaction Lookup")
    wtxs = WithdrawTransaction.objects.select_for_update().select_related('wallet').filter(currency=currency, txid=None).order_by('wallet')

    transaction_hash = {}
    for tx in wtxs:
#        #logger.info("Updating Transaction Hash Amount for TX: {}".format(tx.txid))
        if tx.address in transaction_hash:
            transaction_hash[tx.address] += tx.amount
        else:
            transaction_hash[tx.address] = tx.amount

#        logger.info("Updated Transaction Amount: {}".format(transaction_hash[tx.amount]))

    if currency.dust > Decimal('0'):
#        logger.info("Processing Transaction Dust")
        for address, amount in transaction_hash.items():
            if amount < currency.dust:
                wtxs = wtxs.exclude(currency=currency, address=address)
                del transaction_hash[address]

    if not transaction_hash:
#        logger.warning("No Transaction Hash")
        return

    txid = coin.sendmany(settings.CC_ACCOUNT, transaction_hash)

    if not txid:
#        logger.warning("No Transaction ID")
        return

    fee = coin.gettransaction(txid).get('fee', 0) * -1
#    logger.info("Transaction Fee: {}".format(fee))
    if not fee:
        fee_per_tx = 0
    else:
        fee_per_tx = (fee / len(wtxs))

#    logger.info("Transaction Fee Per TX: {}".format(fee_per_tx))

    fee_hash = defaultdict(lambda : {'fee': Decimal("0"), 'amount': Decimal('0')})
#    logger.info("Fee Hash: {}".format(fee_hash))

    for tx in wtxs:
        fee_hash[tx.wallet]['fee'] += fee_per_tx
        fee_hash[tx.wallet]['amount'] += tx.amount

    for (wallet, data) in fee_hash.iteritems():
        Operation.objects.create(
            wallet=wallet,
            holded=-data['amount'],
            balance=-data['fee'],
            description='Network fee',
            reason=tx
        )

#        logger.info("Updating Wallet balance")
        wallet = Wallet.objects.get(id=tx.wallet.id)
        wallet.balance -= data['fee']
        wallet.holded -= data['amount']
#        logger.info("Wallet<{0}> Balance: {1} Holded {2}".format(wallet.label, wallet.balance, wallet.holded))
        wallet.save()

    wtxs.update(txid=txid, fee=fee_per_tx)
