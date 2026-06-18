
using Microsoft.Data.SqlClient;
using Serilog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using WS_Haimdall.Model_Class;
using static WS_Haimdall.Cache.AppCache;

namespace WS_Haimdall
{
    public class BusinessLayer
    {
        dbLayer dbl ;
        private readonly int PlcNo;

        public BusinessLayer(string conn, int _plcNo)
        {
            dbl = new dbLayer(conn);
            PlcNo = _plcNo;
        }


        public void FillAlarmMaster()
        {
            dict_AlarmTags = GetAlarmMappings();
        }

        //public void FillNodeIdConfig()
        //{
        //    try
        //    {
        //        string[] GroupNames = { "Line_CT", "SubStation_CT", "Production", "SubStation_Losses" };
               

        //        foreach (var eachGrp in GroupNames)
        //        {
        //            if (eachGrp == "Line_CT")
        //                dict_NodeIdConfigLineCT = LoadNodeIdConfig(eachGrp);
        //            else if (eachGrp == "SubStation_CT")
        //                dict_NodeIdConfigSubstationCT = LoadNodeIdConfig(eachGrp);
        //            else if (eachGrp == "Production")
        //                dict_NodeIdConfigLineWiseProdData = LoadNodeIdConfig(eachGrp);
        //            else if (eachGrp == "SubStation_Losses")
        //                dict_NodeIdConfigLosses = LoadNodeIdConfig(eachGrp);


        //        }
        //    }
        //    catch(Exception ex)
        //    {
        //        Log.Error(ex, ex.ToString());
        //    }
            
        //}

        public void FillNodeIdConfig()
        {
            try
            {
                dict_NodeIdConfigLineCT = LoadNodeIdConfig("Line_CT");

                dict_NodeIdConfigSubstationCT = LoadNodeIdConfig("SubStation_CT");

                dict_NodeIdConfigLineWiseProdData = LoadNodeIdConfig("Production");

                dict_NodeIdConfigLosses = LoadNodeIdConfig("SubStation_Losses");

                dict_NodeIdConfigOee = LoadNodeIdConfig("SubStation_OEE");

                dict_NodeIdConfigMTTRMTBF = LoadNodeIdConfig("SubStation_MTTR_MTBF");

            }
            catch (Exception ex)
            {
                Log.Error(ex, ex.ToString());
            }
        }

        //public ConcurrentDictionary<string, string> LoadNodeIdConfig(string GroupName)
        //{
        //    try
        //    {
        //        ConcurrentDictionary<string, string> dict_NodeIdConfg = new ConcurrentDictionary<string, string>();

        //        string query = $@"SELECT [Key], [Value] FROM tbl_Mast_NodeConfg WHERE GroupName = '{GroupName}' AND IsActive = 'true'";

        //        DataSet ds = dbl.ExecSqlDataSet(query, CommandType.Text);

        //        foreach (DataRow row in ds.Tables[0].Rows)
        //        {
        //            string key = row["Key"].ToString();
        //            string value = row["Value"].ToString();

        //            dict_NodeIdConfg[key] = value;
        //        }


        //        return dict_NodeIdConfg;
        //    }
        //    catch (Exception ex)
        //    {
        //        Log.Error(ex, ex.ToString());
        //        return null;
        //    }

        //}

        public ConcurrentDictionary<string, string> LoadNodeIdConfig(string groupName)
        {
            try
            {
                ConcurrentDictionary<string, string> dict_NodeIdConfg =
                    new ConcurrentDictionary<string, string>();

                var listParas = new List<SqlParameter>()
        {
            new SqlParameter("@PlcNo", PlcNo),
            new SqlParameter("@GroupName", groupName)
        };

                DataSet ds = dbl.ExecSqlDataSet(
                    "SP_GetNodeIdConfig",
                    CommandType.StoredProcedure,
                    listParas);

                foreach (DataRow row in ds.Tables[0].Rows)
                {
                    string key = row["Key"].ToString();
                    string value = row["Value"].ToString();

                    dict_NodeIdConfg[key] = value;
                }

                return dict_NodeIdConfg;
            }
            catch (Exception ex)
            {
                Log.Error(ex, ex.ToString());
                return null;
            }
        }

        //public Dictionary<int, string> GetAlarmMappings()
        //{
        //    try
        //    {
        //        Dictionary<int, string> dict_AlarmAddress = new Dictionary<int, string>();

        //        string query = @"
        //SELECT AlarmCode,
        //       Address
        //FROM tbl_Mast_AlarmTags";

        //        DataSet ds = dbl.ExecSqlDataSet(query, CommandType.Text);

        //        foreach (DataRow row in ds.Tables[0].Rows)
        //        {
        //            int alarmCode = Convert.ToInt32(row["AlarmCode"]);
        //            string address = row["Address"].ToString();

        //            dict_AlarmAddress[alarmCode] = address;
        //        }

        //        return dict_AlarmAddress;
        //    }
        //    catch (Exception ex)
        //    {
        //        Log.Error(ex, ex.ToString());
        //        return null;
        //    }

        //}
        public Dictionary<int, string> GetAlarmMappings()
        {
            try
            {
                Dictionary<int, string> dict_AlarmAddress =
                    new Dictionary<int, string>();

                DataSet ds = dbl.ExecSqlDataSet(
                    "SP_GetAlarmMappings",
                    CommandType.StoredProcedure);

                foreach (DataRow row in ds.Tables[0].Rows)
                {
                    int alarmCode = Convert.ToInt32(row["AlarmCode"]);
                    string address = row["Address"].ToString();

                    dict_AlarmAddress[alarmCode] = address;
                }

                return dict_AlarmAddress;
            }
            catch (Exception ex)
            {
                Log.Error(ex, ex.ToString());
                return null;
            }
        }

        public async Task<int> InsertAlarm(object jsonString)
        {
            try
            {
                var listParas = new List<SqlParameter>()
            {

             new SqlParameter("@Json", jsonString)

            };
                return await dbl.ExecSqlNonQuery("SP_Alarm_Bulk_Alarm_FF", CommandType.StoredProcedure, listParas);
            }
            catch (Exception ex)
            {

                Log.Error(ex, ex.ToString());
                return 0;
            }
        }
        public async Task<int> InsertCT(object jsonString)
        {
            try
            {
                var listParas = new List<SqlParameter>()
            {

             new SqlParameter("@Json", jsonString)

            };
                return await dbl.ExecSqlNonQuery("SP_CycleTimeInsert", CommandType.StoredProcedure, listParas);
            }
            catch (Exception ex)
            {
                Log.Error(ex, ex.ToString());
                return 0;
            }
        }

        public async Task<int> InsertLineCT(object jsonString)
        {
            try
            {
                var listParas = new List<SqlParameter>()
            {

             new SqlParameter("@Json", jsonString)

            };
                return await dbl.ExecSqlNonQuery("SP_Insert_CT_Line", CommandType.StoredProcedure, listParas);
            }
            catch (Exception ex)
            {
                Log.Error(ex, ex.ToString());
                return 0;
            }
        }


        public async Task<int> InsertSubStationCT(object jsonString)
        {
            try
            {
                var listParas = new List<SqlParameter>()
            {

             new SqlParameter("@Json", jsonString)

            };
                return await dbl.ExecSqlNonQuery("SP_Insert_CT_SubStation", CommandType.StoredProcedure, listParas);
            }
            catch (Exception ex)
            {
                Log.Error(ex, ex.ToString());
                return 0;
            }
        }

        public async Task<int> InsertLineWiseProdData(object jsonString)
        {
            try
            {
                var listParas = new List<SqlParameter>()
            {

             new SqlParameter("@JsonData", jsonString)

            };
                return await dbl.ExecSqlNonQuery("SP_Insert_Production", CommandType.StoredProcedure, listParas);
            }
            catch (Exception ex)
            {
                Log.Error(ex, ex.ToString());
                return 0;
            }
        }

        public async Task<int> InsertLossesData(object jsonString)
        {
            try
            {
                var listParas = new List<SqlParameter>()
            {

             new SqlParameter("@Json", jsonString)

            };
                return await dbl.ExecSqlNonQuery("SP_InsertUpdate_Losses", CommandType.StoredProcedure, listParas);
            }
            catch (Exception ex)
            {
                Log.Error(ex, ex.ToString());
                return 0;
            }
        }

        public async Task<int> InsertOeeData(object jsonString)
        {
            try
            {
                var listParas = new List<SqlParameter>()
            {

             new SqlParameter("@Json", jsonString)

            };
                return await dbl.ExecSqlNonQuery("SP_InsertUpdate_OEE", CommandType.StoredProcedure, listParas);
            }
            catch (Exception ex)
            {
                Log.Error(ex, ex.ToString());
                return 0;
            }
        }

        public async Task<int> InsertMTTR_MTBFData(object jsonString)
        {
            try
            {
                var listParas = new List<SqlParameter>()
            {

             new SqlParameter("@Json", jsonString)

            };
                return await dbl.ExecSqlNonQuery("SP_InsertUpdate_MTTR_MTBF", CommandType.StoredProcedure, listParas);
            }
            catch (Exception ex)
            {
                Log.Error(ex, ex.ToString());
                return 0;
            }
        }

        public async Task<int> Insert_ErrorLog(object EventName, object Message, object StackTrace)
        {
            try
            {
                var listParas = new List<SqlParameter>()
            {

             new SqlParameter("@EventName", EventName),
             new SqlParameter("@Message", Message),
             new SqlParameter("@StackTrace", StackTrace)

            };
                return await dbl.ExecSqlNonQuery("SP_Insert_ErrorLog", CommandType.StoredProcedure, listParas);
            }
            catch (Exception ex)
            {

                throw;
            }
        }



    }
}

