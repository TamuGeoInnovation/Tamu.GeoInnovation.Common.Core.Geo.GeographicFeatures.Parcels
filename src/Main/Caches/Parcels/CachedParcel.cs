using System;
using USC.GISResearchLab.Common.Addresses;
using USC.GISResearchLab.Common.Geometries.Points;
using USC.GISResearchLab.Common.Geometries.Polygons;

namespace USC.GISResearchLab.Geocoding.Scrapers.LAAssessor.Caches
{
    /// <summary>
    /// Summary description for CachedParcel.
    /// </summary>
    public class CachedParcel : Polygon
    {
        #region Properties
        private int _Hasmultipleassessorids;
        private bool _HasmultipleassessoridsBool;
        private StreetAddress _StreetAddress;

        public StreetAddress StreetAddress
        {
            get { return _StreetAddress; }
            set { _StreetAddress = value; }
        }

        public int Hasmultipleassessorids
        {
            get { return _Hasmultipleassessorids; }
            set { _Hasmultipleassessorids = value; }
        }
        public bool HasmultipleassessoridsBool
        {
            get { return _HasmultipleassessoridsBool; }
            set { _HasmultipleassessoridsBool = value; }
        }
        #endregion



        public CachedParcel()
        {
        }

        public static new CachedParcel FromCoordinateString(string xyString)
        {
            CachedParcel ret = new CachedParcel();
            if (xyString != null && xyString != "")
            {
                string polyLineString = xyString.Trim();
                String[] pointsStrings = xyString.Split(',');
                for (int i = 0; i < pointsStrings.Length; i++)
                {
                    string pair = pointsStrings[i].Trim();
                    Point point = Point.FromString(pair);

                    if (!ret.Contains(point))
                    {
                        ret.AddPoint(point);
                    }
                }
            }

            return ret;
        }

        public void update()
        {
            HasmultipleassessoridsBool = Convert.ToBoolean(Hasmultipleassessorids);
        }
    }
}
