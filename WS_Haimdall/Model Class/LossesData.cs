using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WS_Haimdall.Model_Class
{
    public class LossesData
    {
        public int? ID { get; set; }
        public int? SubStationID { get; set; }
        public string? Shift { get; set; }
        public int? Emergency { get; set; }
        public int? Tip_Change { get; set; }
        public int? Tip_Dress { get; set; }
        public int? OperatorLoading_Starving_Time { get; set; }
        public int? Block_Time { get; set; }
        public int? Manual { get; set; }
        public int? Part_Present_Fault { get; set; }
        public int? RollMoveTime { get; set; }
        public int? LifterMoveTime { get; set; }
        public int? TurnTableMoveTime { get; set; }
        public int? ClampTime { get; set; }
        public int? DeclampTime { get; set; }
        public int? Marriage_Miss_Match { get; set; }
        public int? DropTime { get; set; }
        public int? WeldTime { get; set; }
        public int? PickTime { get; set; }
        public int? SealingTime { get; set; }
        public int? Safety_Gate { get; set; }
        public int? Miscellaneous { get; set; }
        public int? MaterialCall { get; set; }
        public int? Others { get; set; }
        public int? NoOfOccurance { get; set; }
        public int? Total { get; set; }
        public DateTime? Timestamp { get; set; }
        
    }
}
