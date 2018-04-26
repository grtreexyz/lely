using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ServiceStack.OrmLite;
using System.Web;
using System.Data;
using System.Configuration;

namespace lelyModel.DataBase
{
    /// <summary>
    /// 数据库事务类
    /// </summary>
    public class DbSession : IDisposable
    {
        [ThreadStatic]
        static DbSession _CurrentSession;
        public IDbConnection Conn { get; private set; }
        private IDbTransaction DT;
        private bool Commited = false;
        public static DbSession Current { get { return _CurrentSession; } }
        public static bool InDbTrasaction { get { return _CurrentSession != null; } }
        private DbSession() { }

        public static DbSession Begin(string db)
        {
            if (_CurrentSession != null)
            {
                throw new NotSupportedException("当前已有打开的数据库事务");
            }
            var session = new DbSession();
            session.Conn = DbConnFactory.Open(db);
            session.DT = session.Conn.BeginTransaction();
            _CurrentSession = session;

            return _CurrentSession;
        }

        public void Commit()
        {
            if (Commited || DT == null || Conn == null || Conn.State != ConnectionState.Open)
            {
                throw new Exception("事务状态错误,无法提交");
            }
            DT.Commit();
            Commited = true;
        }

        public void Dispose()
        {
            if (DT != null)
            {
                if (!Commited)
                {
                    DT.Rollback();
                }
                DT.Dispose();
            }
            if (Conn != null && Conn.State != ConnectionState.Closed)
            {
                Conn.Close();
                Conn.Dispose();
            }
            if (_CurrentSession != null)
            {
                _CurrentSession = null;
            }
        }
    }

    /// <summary>
    /// 数据库IDbConnection连接类
    /// </summary>
    public class SessionConnetion : IDisposable, IDbConnection
    {
        public SessionConnetion(IDbConnection conn)
        {
            Conn = conn;
        }
        public IDbConnection Conn { get; private set; }

        public string ConnectionString { get { return Conn.ConnectionString; } set { Conn.ConnectionString = value; } }

        public int ConnectionTimeout { get { return Conn.ConnectionTimeout; } }

        public string Database { get { return Conn.Database; } }

        public ConnectionState State { get { return Conn.State; } }

        public IDbTransaction BeginTransaction()
        {
            return Conn.BeginTransaction();
        }

        public IDbTransaction BeginTransaction(IsolationLevel il)
        {
            return Conn.BeginTransaction(il);
        }

        public void ChangeDatabase(string databaseName)
        {
            Conn.ChangeDatabase(databaseName);
        }

        public void Close()
        {
            Conn.Close();
        }

        public IDbCommand CreateCommand()
        {
            return Conn.CreateCommand();
        }

        public void Dispose()
        {
            if (Conn != null && !DbSession.InDbTrasaction)
            {
                Conn.Close();
                Conn.Dispose();
            }
        }

        public void Open()
        {
            Conn.Open();
        }
    }
    /// <summary>
    /// OrmLite连接池类
    /// </summary>
    public static class DbConnFactory
    {
        static ConcurrentDictionary<string, OrmLiteConnectionFactory> factorys = new ConcurrentDictionary<string, OrmLiteConnectionFactory>();
        /// <summary>
        /// 打开连接
        /// </summary>
        /// <param name="db"></param>
        /// <returns></returns>
        public static SessionConnetion Open(string db)
        {
            if (DbSession.InDbTrasaction)
            {
                return new SessionConnetion(DbSession.Current.Conn);
            }
            var newconn = GetFactory(db).OpenDbConnection();
            if (newconn.State != ConnectionState.Open)
            {
                newconn = GetFactory(db).OpenDbConnection();
                if (newconn.State != ConnectionState.Open)
                {
                    throw new AggregateException("无法从连接池中获取已打开的连接");
                }
            }
            return new SessionConnetion(newconn);
        }
        /// <summary>
        /// 在连接池中获取连接，不存在时新建
        /// </summary>
        /// <param name="db"></param>
        /// <returns></returns>
        private static OrmLiteConnectionFactory GetFactory(string db)
        {
            if (factorys.TryGetValue(db, out OrmLiteConnectionFactory temp))
            {
                return temp;
            }
            var conn = ConfigurationManager.ConnectionStrings[db];
            switch (conn.ProviderName)
            {
                case "MySql.Data.MySqlClient":
                    temp = new OrmLiteConnectionFactory(conn.ConnectionString, MySqlDialect.Provider);
                    temp.AutoDisposeConnection = true;
                    break;
                //case "System.Data.SqlClient":
                //    temp = new OrmLiteConnectionFactory(conn.ConnectionString, SqlServerDialect.Provider);
                //    break;
                default:
                    throw new Exception("不支持数据库:" + db);
            }
            factorys.TryAdd(db, temp);
            return temp;
        }
    }
}