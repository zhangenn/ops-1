from dotenv import load_dotenv
load_dotenv()


if __name__ == '__main__':
    from main import update_database
    r = update_database(None, None)
    print(r)
