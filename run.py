"""Running the Xetra ETL application"""
import logging
import logging.config

import yaml

def main():
    """
        entry point to run the xetra ETL job.
    """
    # Parsing YAML file
    config = 'C:/Nishkarsh/xetra_project/Xetra_1234/configs/xetra_report1_config.yml'
    config = yaml.safe_load(open(config))
    # print(config)
    # configure logging
    log_config = config['logging']
    # print(log_config)
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    logger.info("This is a test.")    


if __name__ == '__main__':
    main()