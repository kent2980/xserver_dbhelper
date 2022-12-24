from sshtunnel import SSHTunnelForwarder
import pymysql
import json
from pandas import DataFrame
import os

class NotDBSettingJsonFile(Exception):
    def __str__(self) -> str:
        return "データベースの設定ファイルのパスを設定してください。"

class NotDatabaseSet(Exception):
    def __str__(self) -> str:
        return "接続先のデータベース名を設定してください。"

class DBHelper:
    """xserverのサーバー内のMysqlの操作機能を提供します

    Args:
        database (str, optional): データベース名。初期値はNone。
        db_setting_json_path (str, optional): 設定ファイルのパス文字列。 初期値はNone。

    Raises:
        NotDatabaseSet: データベースの設定が存在しない場合に発生。
        NotDBSettingJsonFile: 設定ファイルが見つからない場合に発生。
        
    Examples:

        ***** 初期化 ********************************************************************
        
        >>> database = "************"
            db_setting_path = "****/*****/*******.json"
            with DBHelper(database, db_setting_path) as helper:
        
        ***** SELECT文を発行 **********************************************************

        >>> sql = "SELECT * FROM TABLE1 WHERE CODE = %s AND NAME = %s"
            args = ["0010","佐藤"]
            
        (a) -> DataFrame 全てのレコードを返却
        
        >>> df = helper.dataframe(sql, args)
        
        (b) -> tuple 全てのレコードを返却
        
        >>> tp = helper.fetch(sql, args)
        
        (c) -> dict 1件ずつ返却
        
        >>> for di in helper.fetchItem(sql,args):
                print(di)
                
        ***** INSERT,UPDATE,DELETE文を発行 ******************************************
        
        >>> sql = "INSERT INTO TABLE1 (CODE, NAME) VALUE (%s, %s)
            args = ["0010", "佐藤"]
            many_args = [["0010", "佐藤"], ["0020", "高橋"]]
            
        (a) -> int 1件のデータを更新
        
        >>> i = helper.execute(sql, args)
        
        (b) -> int まとめてデータを更新
        
        >>> i = helper.executemany(sql, many_args)
            
    """

    def __init__(self, database:str=None, db_setting_json_path:str=None):
        """xserverのサーバー内のMysqlの操作機能を提供します

        Args:
            database (str, optional): データベース名。初期値はNone。
            db_setting_json_path (str, optional): 設定ファイルのパス文字列。 初期値はNone。

        Raises:
            NotDatabaseSet: データベースの設定が存在しない場合に発生。
            NotDBSettingJsonFile: 設定ファイルが見つからない場合に発生。
            
        Examples:

            ***** 初期化 ********************************************************************
            
            >>> database = "************"
                db_setting_path = "****/*****/*******.json"
                with DBHelper(database, db_setting_path) as helper:
            
            ***** SELECT文を発行 **********************************************************

            >>> sql = "SELECT * FROM TABLE1 WHERE CODE = %s AND NAME = %s"
                args = ["0010","佐藤"]
                
            (a) -> DataFrame 全てのレコードを返却
            
            >>> df = helper.dataframe(sql, args)
            
            (b) -> tuple 全てのレコードを返却
            
            >>> tp = helper.fetch(sql, args)
            
            (c) -> dict 1件ずつ返却
            
            >>> for di in helper.fetchItem(sql,args):
                    print(di)
                    
            ***** INSERT,UPDATE,DELETE文を発行 ******************************************
            
            >>> sql = "INSERT INTO TABLE1 (CODE, NAME) VALUE (%s, %s)
                args = ["0010", "佐藤"]
                many_args = [["0010", "佐藤"], ["0020", "高橋"]]
                
            (a) -> int 1件のデータを更新
            
            >>> i = helper.execute(sql, args)
            
            (b) -> int まとめてデータを更新
            
            >>> i = helper.executemany(sql, many_args)
                
        """
        # データベース名がNoneな場合例外を投げる
        if database is None:
            raise NotDatabaseSet()
        else:
            self.db_name = database
            
        # 設定ファイルの例外処理
        try:
            
            # JSONファイルからデータを受け取る
            with open(db_setting_json_path, 'r', encoding='utf-8') as json_file:
                json_data = json.load(json_file)
            
            # データベース設定を取り出す 
            self.db_host = json_data[u"db_host"]
            self.db_user = json_data[u"db_user"]
            self.db_password = json_data[u"db_password"]
            self.ssh_pkey = os.path.join(os.path.dirname(db_setting_json_path), json_data[u"ssh_pkey_name"])
            print(self.ssh_pkey)
            self.ssh_host = json_data[u"ssh_host"]
            self.ssh_port = int(json_data[u"ssh_port"])
            self.ssh_user = json_data[u"ssh_user"]
            self.ssh_pkey_pass = json_data[u"ssh_pkey_pass"]
            self.ssh_mysql_host = json_data[u"ssh_mysql_host"]
            self.ssh_mysql_port = int(json_data[u"ssh_mysql_port"])
            
        except Exception:
            raise NotDBSettingJsonFile()
        
    def __enter__(self):
        # SShサーバーへ接続します
        self.server = SSHTunnelForwarder(
            (self.ssh_host, self.ssh_port),
            ssh_username=self.ssh_user,
            ssh_private_key_password=self.ssh_pkey_pass,
            ssh_pkey=self.ssh_pkey,
            remote_bind_address=(self.ssh_mysql_host,self.ssh_mysql_port),
        )
        self.server.start()
        self.con = pymysql.connect(
            host=self.db_host,
            port=self.server.local_bind_port,
            user=self.db_user,
            passwd=self.db_password,
            db=self.db_name,
            # Select結果をタブルではなく辞書で受け取る
            cursorclass=pymysql.cursors.DictCursor
        )
        self.cur = self.con.cursor()
        return self
        
    def __exit__(self, exc_type, exc_value, traceback):
        self.con.close()
        self.server.stop()
    
        
    def fetch(self, sql:str, args=None) -> tuple:        
        """SQLクエリで抽出した全てのレコードを返却します。
        
        Note:
            大きなデータを取得する際は、大量のメモリを消費するので注意。

        Args:
            sql (str): SQLクエリ

        Returns:
            tuple: レコードセット
        
        Examples:
            >>> sql = "SELECT * FROM TABLE1 WHERE CODE = %s AND NAME = %s"
                args = ["0010", "佐藤"]
                result = fetch(sql, args)
        """
        self.cur.execute(sql, args)
        result = self.cur.fetchall()
        return result
    
    def fetchItem(self, sql:str, args=None) -> dict:
        """SQLクエリで抽出したレコードを1件ずつ返します。

        Args:
            sql (str): SQLクエリ
            args : 置換文字列

        Returns:
            dict: generator object DBHelper.fetchItem

        Yields:
            Iterator[dict]: レコードセット（辞書型）
            
        Examples:
            >>> sql = "SELECT * FROM TABLE1 WHERE CODE = %s AND NAME = %s"
                args = ["0010", "佐藤"]
                for item in fetchItem(sql, args):
                    print(item)
                    

        """
        # SQLクエリをリクエストする
        self.cur.execute(sql, args)
        # ループ処理
        while True:
            # 1行ずつクエリ結果を取り出す。
            result = self.cur.fetchone()            
            # データが無くなったらループを抜ける
            if result is None:
                break            
            # 1行ずつ結果を返却
            yield result

    def execute(self, sql:str, args:tuple=None) -> int:
        """SQLクエリで1件のデータを更新します。

        Args:
            sql (str): SQLクエリ
            args (tuple): _description_. Defaults to None.
        
        Returns:
            int: 更新件数
            
        Examples:
            >>> sql = "INSERT INTO TABLE1 (CODE, NAME) VALUE (%s, %s)
                args = ["0010", "佐藤"]
                i = excute(sql, args)
        """
        self.cur.execute(sql, args)
        self.con.commit()

    def executemany(self, sql:str, list:list) -> int:
        """SQLクエリでまとめてデータの更新を行います。

        Args:
            sql (str): SQLクエリ
            list (list): 更新データ

        Returns:
            int: 更新件数
        
        Examples:
            >>> sql = "INSERT INTO TABLE1 (CODE, NAME) VALUE (%s, %s)"
                list = [["0010", "佐藤"], ["0020", "高橋"]]
                i = executemany(sql, list)
        """
        self.cur.executemany(sql, list)
        self.con.commit()
        
    def dataframe(self, sql:str, args=None) -> DataFrame:
        """SQLクエリで抽出したすべてのレコードをDataFrameで出力します。

        Args:
            sql (str): SQLクエリ
            args (tuple): 置換文字列

        Returns:
            DataFrame: レコードセット
        
        Examples:
            >>> sql = "SELECT * FROM TABLE1 WHERE CODE = %s AND NAME = %s"
                args = ["0010", "佐藤"]
                df = fetch(sql, args)
        """
        df = DataFrame(self.fetch(sql, args))
        return df

if __name__ == "__main__":

    path = "******/*****/*****.json"
    args = ("0010","")
    with DBHelper("*****", path) as main:
        brands = main.dataframe("select * from brands")
        brands.to_csv("***.csv") 