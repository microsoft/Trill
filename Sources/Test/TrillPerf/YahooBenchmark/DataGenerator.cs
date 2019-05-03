// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceTesting.YahooBenchmark
{
    public enum Ad_Type { Banner, Modal, SponsoredSearch, Mail, Mobile }

    public enum Event_Type { View, Click, Purchase }

    public struct Event
    {
        private static long Count = 0;
        private static readonly int Ad_Count = Enum.GetValues(typeof(Ad_Type)).Length;
        private static readonly int Event_Count = Enum.GetValues(typeof(Event_Type)).Length;

        public Guid user_id;
        public Guid page_id;
        public Guid ad_id;
        public Ad_Type ad_type;
        public Event_Type event_type;
        public DateTime event_time;
        public string ip_address;

        public static Event CreateEvent(Guid[] ads)
        {
            var guid = Guid.NewGuid();

            var ev = new Event
            {
                user_id = guid,
                page_id = guid,
                ad_id = ads[Count % ads.Length],
                ad_type = (Ad_Type)(Count % Ad_Count),
                event_type = (Event_Type)(Count % Event_Count),
                event_time = DateTime.Now,
                ip_address = "255.255.255.255"
            };

            Count++;

            return ev;
        }
    }

    public struct Output
    {
        public DateTime time_window;
        public Guid campaign_id;
        public ulong count;
        public DateTime lastUpdate;
    }

    public struct ProjectedEvent
    {
        public Guid ad_id;
        public Guid campaign_id;
        public DateTime event_time;
    }

    public static class DataGenerator
    {
        public static readonly Dictionary<Guid, Guid> Campaigns = GenerateCampaigns();
        public static readonly Guid[] Ads = Campaigns.Keys.ToArray();

        private const int CampaignCount = 100;
        private const int AdsPerCampaign = 10;

        private static Dictionary<Guid, Guid> GenerateCampaigns()
        {
            var dict = new Dictionary<Guid, Guid>(CampaignCount);

            for (int campaign = 0; campaign < CampaignCount; campaign++)
            {
                var campaignGuid = Guid.NewGuid();

                for (int ad = 0; ad < AdsPerCampaign; ad++)
                {
                    var ad_Guid = Guid.NewGuid();

                    dict.Add(ad_Guid, campaignGuid);
                }
            }

            return dict;
        }
    }
}
