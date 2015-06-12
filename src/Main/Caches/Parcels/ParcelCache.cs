using System;
using System.Data;
using System.Data.SqlClient;
using USC.GISResearchLab.Common.Addresses;
using USC.GISResearchLab.Common.Databases;
using USC.GISResearchLab.Common.Utils.Databases;
using USC.GISResearchLab.Common.Utils.Strings;

namespace USC.GISResearchLab.Geocoding.Scrapers.LAAssessor.Caches.Parcels
{
    public class ParcelCache : DatabaseTableSource
    {
        public ParcelCache(string connectionString, string tableName)
            : base(connectionString, tableName)
        { }

        public ParcelCache(string dataSource, string catalog, string userName, string password, string tableName)
            : base(dataSource, catalog, userName, password, tableName)
        { }

        public CachedParcel checkExtractedParcelCache(StreetAddress address)
        {
            return checkExtractedParcelCache(address.Number, address.PreDirectional, address.StreetName, address.Suffix, address.PostDirectional, address.City, address.State, address.ZIP);
        }

        public CachedParcel checkExtractedParcelCache(StreetAddress address, string streetNumber)
        {
            return checkExtractedParcelCache(streetNumber, address.PreDirectional, address.StreetName, address.Suffix, address.PostDirectional, address.City, address.State, address.ZIP);
        }

        public CachedParcel checkExtractedParcelCache(string number, string pre, string name, string suffix, string post, string city, string state, string zip)
        {
            CachedParcel cachedParcel = null;
            StreetAddress streetAddress = new StreetAddress(number, pre, name, suffix, post, city, state, zip);

            if (number != null && number != "" && name != null && name != "")
            {

                Read();

                if (StringUtils.IsEmpty(number))
                {
                    number = "0";
                }

                string sql = "select id, hasmultipleassessorids, assessorid ";
                sql += " from " + TableName;
                sql += " where ";
                sql += " number='" + number + "' ";
                sql += DatabaseUtils.ParameterOrBlank("pre", pre, true, true);
                sql += "and name='" + DatabaseUtils.AsDbString(name) + "' ";
                sql += DatabaseUtils.ParameterOrBlank("suffix", suffix, true, true);
                sql += DatabaseUtils.ParameterOrBlank("post", post, true, true);
                sql += "and State='" + state + "' ";
                sql += "and ( (city = '" + city + "') " +
                    " OR " +
                    " (zip = '" + zip + "')) ";

                try
                {


                    SqlCommand cmd1 = new SqlCommand(sql, OpenedConnection);
                    cmd1.CommandType = CommandType.Text;
                    cmd1.CommandTimeout = 0;
                    SqlDataReader dr = cmd1.ExecuteReader();
                    if (dr.HasRows)
                    {
                        Hit();
                        while (dr.Read())
                        {
                            cachedParcel = new CachedParcel();

                            cachedParcel.Id = dr.GetInt32(0);
                            cachedParcel.Hasmultipleassessorids = dr.GetInt32(1);
                            streetAddress.AddressId = dr.GetString(2);
                            cachedParcel.StreetAddress = streetAddress;
                            cachedParcel.update();
                        }
                    }
                    else
                    {
                        Miss();
                    }

                    dr.Close();

                }
                catch (Exception e)
                {
                    throw new Exception(e.Message + " - at: " + e.StackTrace + " : " + sql);
                }

            }
            return cachedParcel;

        }

        public void cacheParcel(string assessorId, string number, string pre, string name, string suffix, string post, string city, string state, string zip, int hasMultipleAssessorIds)
        {

            if (StringUtils.IsEmpty(number))
            {
                number = "0";
            }

            Write();

            string sql = "";
            sql += "insert into la_assessor_cache_parcels ";
            sql += " ( ";
            sql += " hasMultipleAssessorIds, ";
            sql += " assessorId, ";
            sql += " number, ";
            sql += " pre, ";
            sql += " name, ";
            sql += " suffix, ";
            sql += " post, ";
            sql += " city, ";
            sql += " State, ";
            sql += " added, ";
            sql += " zip ";
            sql += " ) VALUES ";
            sql += " (";
            sql += " '" + hasMultipleAssessorIds + "',";
            sql += " '" + assessorId + "',";
            sql += " '" + number + "',";
            sql += " '" + DatabaseUtils.AsDbString(pre) + "',";
            sql += " '" + DatabaseUtils.AsDbString(name) + "',";
            sql += " '" + DatabaseUtils.AsDbString(suffix) + "',";
            sql += " '" + DatabaseUtils.AsDbString(post) + "',";
            sql += " '" + DatabaseUtils.AsDbString(city) + "',";
            sql += " '" + state + "',";
            sql += " '" + DateTime.Now + "',";
            sql += " '" + zip + "' ";
            sql += " )";

            try
            {
                SqlCommand cmd = new SqlCommand(sql, OpenedConnection);
                cmd.CommandType = CommandType.Text;
                cmd.CommandTimeout = 0;
                cmd.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                throw new Exception("uniform-lot method error: " + e.Message + " - at: " + e.StackTrace + " : " + sql);
            }
        }



        public int CalculateNumberOfLots(ValidateableStreetAddress address, int from, int to)
        {
            Read();
            int ret = 0;
            int evenOdd = address.NumberInt % 2;

            string sql = "";
            sql += "select count(*) from la_assessor_cache_parcels " +
                " where " +
                " name = '" + DatabaseUtils.AsDbString(address.StreetName) + "' ";

            sql += DatabaseUtils.ParameterOrBlank("pre", address.PreDirectional, true, true);
            sql += DatabaseUtils.ParameterOrBlank("suffix", address.Suffix, true, true);
            sql += DatabaseUtils.ParameterOrBlank("post", address.PostDirectional, true, true);
            sql += DatabaseUtils.ParameterOrBlank("state", address.State, true, true);

            sql += " and " +
                "( (city = '" + address.City + "') " +
                " OR " +
                " (zip = '" + address.ZIP + "')) and " +
                " (number >= '" + Math.Min(from, to) + "' and number <= '" + Math.Max(from, to) + "')" +
                " and convert(int,number%2) = " + evenOdd;
            try
            {
                SqlCommand cmd = new SqlCommand(sql, OpenedConnection);
                cmd.CommandType = CommandType.Text;
                cmd.CommandTimeout = 0;
                SqlDataReader dr = cmd.ExecuteReader();
                if (dr.HasRows)
                {
                    Hit();
                    while (dr.Read())
                    {
                        ret = dr.GetInt32(0);
                    }
                }
                else
                {
                    Miss();
                }
                dr.Close();

            }
            catch (Exception e)
            {
                throw new Exception("Problem calculating numberOfLots from cache: " + e.Message + " - at: " + e.StackTrace + " : " + sql);
            }


            return ret;
        }

    }
}
