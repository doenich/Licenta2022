from src.scraping import main as scraping
from src.data_transfer import main as data_transfer
from src.processing import main as processing
from src.aggregation import main as aggregation
from src.visualization import main as visualization


def main():
    """Entry point."""
    # scraping()
    data_transfer()

if __name__ == '__main__':
    main()