import logging

def init_log(ad_set_id):
    name = ad_set_id + ' log'
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        file_name = './' + ad_set_id + ' op_log.txt'
        ch = logging.FileHandler(file_name, encoding="UTF-8")
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        # logger.removeHandler(ch)
    return logger