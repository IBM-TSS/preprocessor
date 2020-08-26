from fetchers.WarningsFetcher import WarningsFetcher
from preprocessors.WarningsPreprocesor import WarningsPreprocesor
from utils.ShortRunDB import ShortRunDB


class LoadTickets:

    @staticmethod
    def process():

        # Use the fetcher to get the backlog file.
        # WarningsFetcher.process()

        # Process the file to get the parsed open tickets.
        open_tickets = WarningsPreprocesor.process()

        # Use the Corpus API to upload the open tickets
        with ShortRunDB() as db:
            db.load_open_tickets(open_tickets)
