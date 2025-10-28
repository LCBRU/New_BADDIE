import time
from alive_progress import alive_bar

    with alive_bar(10, title='Outer Loop') as outer_bar:
        for _ in range(10):
            with alive_bar ( 5, title='Inner Loop' ) as inner_bar:
                for _ in range ( 5 ):
                    time.sleep ( 0.2 )  # Simulate work
                    inner_bar ()

if __name__ == "__main__":
    outer_loop()