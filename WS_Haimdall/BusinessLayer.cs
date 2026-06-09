
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using System.Data;
using static WS_Haimdall.Cache.AppCache;
using Serilog;

namespace WS_Haimdall
{
    public class BusinessLayer
    {
        dbLayer dbl ;
        public BusinessLayer(string conn)
        {
            dbl = new dbLayer(conn);
        }


        public void FillAlarmMaster()
        {
            dict_AlarmTags = dbl.GetAlarmMappings();
        }

        public void FillNodeIdConfig()
        {
            try
            {
                string[] GroupNames = { "Line CT", "SubStation CT", "Production" };

                foreach (var eachGrp in GroupNames)
                {
                    if (eachGrp == "Line CT")
                        dict_NodeIdConfigLineCT = dbl.LoadNodeIdConfig(eachGrp);
                    else if (eachGrp == "SubStation CT")
                        dict_NodeIdConfigSubstationCT = dbl.LoadNodeIdConfig(eachGrp);
                    else if (eachGrp == "Production")
                        dict_NodeIdConfigLineWiseProdData = dbl.LoadNodeIdConfig(eachGrp);

                }
            }
            catch(Exception ex)
            {
                Log.Error(ex, ex.ToString());
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

