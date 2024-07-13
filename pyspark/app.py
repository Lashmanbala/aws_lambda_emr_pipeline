from util import get_spark_session
import os

def main():
    env = os.environ.get('ENVIRON')

    spark = get_spark_session(env, 'Github')

    spark.sql('SELECT current_date').show()

if __name__ == '__main__':
    main()
