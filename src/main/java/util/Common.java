package util;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/3 14:01
 */
public class Common {
    public static final int PARALLELISM = 3;
    public static final int WINDOW_SIZE_SECONDS = 10;
    public static final int HBASE_POOL_MAX_IDLE = 5;
    public static final int HBASE_POOL_MAX_ACTIVE = 10;
    public static final int REDIS_EXPIRE_SECONDS = 60 * 60 * 24;
    public static final String KAFKA_SERVERS = "hadoop102:9092"; // properties better？
    public static final String CHECKPOINT_PATH = "hdfs://hadoop102:8020/flink-gmall/ck";
    public static final String TOPIC_ODS_DB = "topic_db";
    public static final String KAFKA_DIM_GROUP = "dim_app_group";
    public static final String KAFKA_DWD_LOG_GROUP = "dwd_log_split_group";
    public static final String KAFKA_DWS_KEYWORD_SPLIT_GROUP = "dws_keyword_split_group";
    public static final String KAFKA_DWS_HOME_DETAIL_UV_GROUP = "dws_home_detail_pv_uv_group";
    public static final String KAFKA_DWS_CALLBACK_UV_GROUP = "dws_page_callback_uv_group";
    public static final String KAFKA_DWD_COMMENT_INFO_GROUP = "dwd_comment_info_group";
    public static final String KAFKA_DWD_PAY_SUC_GROUP = "dwd_pay_suc_group";
    public static final String KAFKA_DWD_TRADE_ORDER_DETAIL_GROUP = "dws_keyword_split_group";
    public static final String KAFKA_DWD_CARD_ADD_GROUP = "dwd_cart_add_group";
    public static final String TOPIC_ODS_LOG = "topic_log";

    public static final String MYSQL_HOST = "hadoop102";
    public static final int MYSQL_PORT = 3306;

    public static final String MYSQL_USERNAME = "root";

    public static final String MYSQL_PASSWORD = "000000";

    public static final String HBASE_NAMESPACE = "gmall_flink";

    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";

    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";

    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";
    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
    public static final String TOPIC_DWD_INTERACTION_COMMENT_INF = "dwd_interaction_comment_info";

    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";

    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";

    public static final String TOPIC_DWD_TRADE_CANCEL_DETAIL = "dwd_trade_cancel_detail";

    public static final String TOPIC_DWD_TRADE_PAY_DETAIL_SUC = "dwd_trade_pay_detail_suc";

    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund";

    public static final String TOPIC_DWD_TRADE_REFUND_PAY_SUC = "dwd_trade_refund_pay_suc";

    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";

    public static final String MYSQL_URL = "jdbc:mysql://hadoop102:3306?allowPublicKeyRetrieval=true&useSSL=false";

    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register";
    public static final String KAFKA_DWD_USER_REGISTER_GROUP = "dwd_user_register_group";

    public static final String KAFKA_DWD_TRADE_PAY_DETAIL_SUC_GROUP = "dwd_trade_pay_detail_suc";

    public static final int ONE_DAY = 24 * 60 * 60;

    public static final String DORIS_FE_NODES = "hadoop102:7030";
    public static final String DORIS_USERNAME = "root";
    public static final String DORIS_PASSWORD = "";

}
