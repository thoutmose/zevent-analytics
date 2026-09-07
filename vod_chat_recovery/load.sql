\copy bronze_live_chat(batch_id,row_number,channel,chatter,chatter_id,message_text,message_sent_at,captured_at,badges,emotes,account_created_at) from '/backfill_rows.csv' with (format csv)
