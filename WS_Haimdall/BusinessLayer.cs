
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using System.Data;

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
            AlarmTags.dict_AlarmTags = dbl.GetAlarmMappings();
        }

        public void FillNodeIdConfig()
        {
            string[] GroupNames = { "Line CT", "SubStation CT" };


            foreach(var eachGrp in GroupNames)
            {
                if(eachGrp == "Line CT")
                    NodeIdConfig.dict_NodeIdConfigLineCT = dbl.LoadNodeIdConfig(eachGrp);
                else if (eachGrp == "SubStation CT")
                    NodeIdConfig.dict_NodeIdConfigSubstationCT = dbl.LoadNodeIdConfig(eachGrp);

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

                //await Insert_ErrorLog("Update_Status_Received", ex.Message, ex.StackTrace);
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

                //await Insert_ErrorLog("Update_Status_Received", ex.Message, ex.StackTrace);
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

                //await Insert_ErrorLog("Update_Status_Received", ex.Message, ex.StackTrace);
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


        //public DataSet BindRunningParts()
        //{
        //    return dbl.ExecSqlDataSet("SP_GetRunningPartID", CommandType.StoredProcedure);
        //}

        //public object MaxSessionByVC(object PartID)
        //{
        //    try
        //    {
        //        var listParas = new List<SqlParameter>()
        //    {
        //     new SqlParameter("@PartID", PartID)

        //    };
        //        return dbl.ExecSqlScalar("select dbo.SVF_GetMaxSessionNoByVC(@PartID) as MaxSessionNo", CommandType.Text, listParas);
        //    }
        //    catch (Exception ex)
        //    {

        //        throw;
        //    }
        //}
        //// USer Station Verify
        //public DataSet GetPartConfigByID(object PartID)
        //{
        //    var listParas = new List<SqlParameter>()
        //    {

        //     new SqlParameter("@PartID", PartID)
        //    // new SqlParameter( "@LogDate", LogDate),


        //    };
        //    return dbl.ExecSqlDataSet("SP_GetPartUserByVariantCode", CommandType.StoredProcedure, listParas);
        //}
        //// Tool Verify
        //public DataSet GetToolBarcodeBYPartID(object PartID)
        //{
        //    var listParas = new List<SqlParameter>()
        //    {

        //     new SqlParameter("@PartID", PartID)
        //    // new SqlParameter( "@LogDate", LogDate),


        //    };
        //    return dbl.ExecSqlDataSet("SP_GetToolBarcodeBYPartID", CommandType.StoredProcedure, listParas);
        //}


        //public DataSet IspartRunning(object PartID)
        //{
        //    var listParas = new List<SqlParameter>()
        //    {
        //     new SqlParameter("@PartID", PartID)
        //    // new SqlParameter( "@LogDate", LogDate),

        //    };
        //    return dbl.ExecSqlDataSet("SP_IsPartRunning", CommandType.StoredProcedure, listParas);
        //}
        //public DataSet IsProcessComplete()
        //{

        //    return dbl.ExecSqlDataSet("SP_IsProcessComplete", CommandType.StoredProcedure);
        //}

        //public int GetToolIDbyStationID(object StationID)
        //{
        //    try
        //    {
        //        var listParas = new List<SqlParameter>()
        //    {

        //     new SqlParameter("@StationID", StationID)

        //    };
        //        return Convert.ToInt32(dbl.ExecSqlScalar("Sp_GetToolIDByStationID", CommandType.StoredProcedure, listParas));
        //    }
        //    catch (Exception ex)
        //    {

        //        throw;
        //    }
        //}

    }
}

