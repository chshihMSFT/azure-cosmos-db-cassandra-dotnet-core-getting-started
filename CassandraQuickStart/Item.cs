using System;

namespace CassandraQuickStart
{
    /**
     * User Item entity class
     */
    public class Item
    {
        public String item_category { get; set; }
        public String item_id { get; set; }
        public String item_name { get; set; }
        public String item_createTime { get; set; }

        public Item(String item_category, String item_id, String item_name, String item_createTime)
        {
            this.item_category = item_category;
            this.item_id = item_id;
            this.item_name = item_name;
            this.item_createTime = item_createTime;
        }
        public override String ToString()
        {
            return String.Format(" {0} | {1} | {2} | {3} ", item_category, item_id, item_name, item_createTime);
        }
    }
}
