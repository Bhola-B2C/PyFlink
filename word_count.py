import logging
import os
import shutil
import sys
import tempfile

from add import add
import wikipedia

from pyflink.dataset import ExecutionEnvironment
from pyflink.table import TableConfig, DataTypes, BatchTableEnvironment
from pyflink.table.descriptors import Schema, OldCsv, FileSystem


def word_count():
    result = wikipedia.page("New York City")
    content = result.summary


    t_config = TableConfig()
    env = ExecutionEnvironment.get_execution_environment()
    t_env = BatchTableEnvironment.create(env, t_config)
    print(add.add(10,5))
    print("Word Count");
    # register Results table in table environment
    tmp_dir = tempfile.gettempdir()
    result_path = tmp_dir + '/result'
    if os.path.exists(result_path):
        try:
            if os.path.isfile(result_path):
                os.remove(result_path)
            else:
                shutil.rmtree(result_path)
        except OSError as e:
            logging.error("Error removing directory: %s - %s.", e.filename, e.strerror)

    logging.info("Results directory: %s", result_path)

    #sink_ddl = """
    #    create table Results(
    #        word VARCHAR,
    #        `count` BIGINT
    #    ) with (
    #        'connector.type' = 'filesystem',
    #        'format.type' = 'csv',
    #        'connector.path' = '{}'
    #   )
    #    """.format(result_path)
    t_env.connect(FileSystem().path('/tmp/output')) \
    .with_format(OldCsv()
                 .field_delimiter('\t')
                 .field('word', DataTypes.STRING())
                 .field('count', DataTypes.BIGINT())) \
    .with_schema(Schema()
                 .field('word', DataTypes.STRING())
                 .field('count', DataTypes.BIGINT())) \
    .create_temporary_table('Results')
    #t_env.sql_update(sink_ddl)

    elements = [(word, 1) for word in content.split(" ")]
    t_env.from_elements(elements, ["word", "count"]) \
         .group_by("word") \
         .select("word, count(1) as count") \
         .insert_into("Results")

    t_env.execute("word_count")


#if __name__ == '__main__':
#    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

 #   word_count()
