if exists (select * from sysobjects where name = 'sp_update_ncoa_address' and type = 'p')
	drop proc sp_update_ncoa_address
go

create proc sp_update_ncoa_address
@asofdate		datetime	--the date when ncoa export happens
as

--for update imis address with ncoa
--jx, used for importing NCOA address into iMIS
--creates a log record if NCOA succeeds for the ID
--jx, exceptions of incorrect NCOA update

declare @id				varchar(10)
declare @addressid		int
declare @err			int
declare @new_full_address varchar(255)
declare @ncoa_mtype		varchar(1)
declare @company		varchar(80)

set nocount on

declare cr_ncoa cursor
forward_only read_only
for

--jx,checking EXHIBIT ONLY and others in name
select clientid, addressid, ncoa_mtype, n.company
from ncoa, name n, name_address na, name_ascd ascd
where ncoa.clientid = n.id
  and na.address_num = ncoa.addressid
  and na.id = ncoa.clientid
  and n.id = ascd.id		--jx,exclude bookstore
  and n.title <> 'SWETS'	--exhibit, distrubutor, swets excluded from update
  and n.title not like 'EXHIBIT%'
  and n.title not like 'DISTRIBUTOR%'
  --the date is when the NCOA export was done
  and (na.last_updated is null or na.last_updated < @asofdate)
  and isnull(ncoa.new_add1,'') <> ''	--jx,modified to check blank field besides null
  and isnull(ncoa.new_city,'') <> ''
  and isnull(ncoa.new_state,'') <> ''
  and isnull(ncoa.new_zip,'') <> ''
  and ascd.bookstore = 0
  and ncoa.status = 0
  and n.id not in ('22218')	--jx, exceptions of incorrect NCOA update
--   and ncoa.clientid like '1000%'
--   and ncoa.clientid = '1002534'
order by clientid, addressid

open cr_ncoa
fetch next from cr_ncoa into @id, @addressid, @ncoa_mtype, @company

-- select @id, @addressid

while @@fetch_status = 0
begin
  
  select @err = 0	

  BEGIN TRAN

	insert name_log
	(date_time, log_type, sub_type, user_id, id, log_text)
	select getdate(), 'Change', 'Change', 'NCOA', @id, 
	na.purpose + ', ' + na.full_address + ' -> ' + 
	(CASE WHEN ncoa.new_add1 <> '' THEN ncoa.new_add1 else '' END) +
      (CASE when ncoa.new_add2 is null then ''
			when ncoa.new_add2 = '' then ''
			WHEN ncoa.new_add2 <> '' THEN char(13) + ncoa.new_add2 
			else '' END) +
      rtrim(CASE WHEN ncoa.new_city <> '' THEN char(13) + ncoa.new_city else '' END) +
          (CASE WHEN ncoa.new_state <> '' THEN ', ' + ncoa.new_state else '' END) +
          (CASE when ncoa.new_zip4 is null then ' ' + ncoa.new_zip
			 when ncoa.new_zip4 = '' then ' ' + ncoa.new_zip					 
			 when ncoa.new_zip <> '' and ncoa.new_zip4 <> '' then ' ' + rtrim(ncoa.new_zip) + '-' + ncoa.new_zip4
			 else '' end)
	from name_address na, ncoa
	where na.id = @id
	  and na.address_num = @addressid
	  and na.id = ncoa.clientid
	  and na.address_num = ncoa.addressid
 	if @@error <> 0
		select @err = @err + 1

	--insert nixie record
	insert nixie
	(id, address_1, address_2, city, state_province, zip, country, full_address,
	postal_changes, status_changes, requests, data_issues, source, 
	date_time, entered_by)
	select 
	@id, na.address_1, na.address_2, na.city, na.state_province, na.zip, na.country, 
	na.full_address, 'Change of Address', '', '', '', '', 
	getdate(), 'NCOA'
	from name_address na
	where na.id = @id
	  and na.address_num = @addressid

	if @@error <> 0 
		select @err = @err + 1

	--nixie audit trail, jx
	insert name_log
	(date_time, log_type, sub_type, [user_id], id, log_text)
	select getdate(), 'Change', 'Delete', 'NCOA', @id, 
		'Nixie, ' + na.purpose + ', ' + na.full_address
	from name_address na
	where na.id = @id
	  and na.address_num = @addressid

	if @@error <> 0 
		select @err = @err + 1

-- 	select ncoa.*
	update na
	set address_1 = ncoa.new_add1,
	  address_2 = isnull(ncoa.new_add2,''),
	  city = ncoa.new_city,
	  state_province = ncoa.new_state,
	  zip = CASE when ncoa.new_zip4 is null then ncoa.new_zip
				 when ncoa.new_zip4 = '' then ncoa.new_zip					 
				 when ncoa.new_zip <> '' and ncoa.new_zip4 <> '' 
					then rtrim(ncoa.new_zip) + '-' + ncoa.new_zip4
				 else '' end,
      full_address =  
         (CASE WHEN ncoa.new_add1 <> '' THEN ncoa.new_add1 else '' END) +
          (CASE when ncoa.new_add2 is null then ''
				when ncoa.new_add2 = '' then ''
				WHEN ncoa.new_add2 <> '' THEN char(13) + ncoa.new_add2 
				else '' END) +
          rtrim(CASE WHEN ncoa.new_city <> '' THEN char(13) + ncoa.new_city else '' END) +
	          (CASE WHEN ncoa.new_state <> '' THEN ', ' + ncoa.new_state else '' END) +
	          (CASE when ncoa.new_zip4 is null then ' ' + ncoa.new_zip
				 when ncoa.new_zip4 = '' then ' ' + ncoa.new_zip					 
				 when ncoa.new_zip <> '' and ncoa.new_zip4 <> '' then ' ' + rtrim(ncoa.new_zip) + '-' + ncoa.new_zip4
				 else '' end),
	  mail_code = '',		--jx
	  last_updated = getdate()
	from name_address na, ncoa
	where na.id = @id
	  and na.address_num = @addressid
	  and na.id = ncoa.clientid
	  and na.address_num = ncoa.addressid

	if @@rowcount = 0 or @@error <> 0
		select @err = @err + 1

	update ncoa
	set status = 1
	where clientid = @id
	  and addressid = @addressid

	if @@error <> 0
		select @err = @err + 1

	update n
	set full_address = na.full_address,
	  city = na.city,
	  state_province = na.state_province,
	  zip = na.zip,
	  last_updated = getdate(),
	  updated_by = 'NCOA',
	  chapter = case when na.STATE_PROVINCE in
				        (select PRODUCT_MINOR from Product 
						where PRODUCT_MAJOR = 'CHAPT')
	          		then na.STATE_PROVINCE
					else '' end,		--jx,update ren region
	  company = case when (not exists 
						(select * from name_address na2
						where na2.id = n.id
						  and na2.purpose = 'OFFICE'))
						and @ncoa_mtype = 'I'
						and n.company_record = 0
					then ''
					else n.company end,	--jx,remove company field for certain records
	  company_sort = case when (not exists 
						(select * from name_address na2
						where na2.id = n.id
						  and na2.purpose = 'OFFICE'))
						and @ncoa_mtype = 'I'
						and n.company_record = 0
					then ''
					else n.company_sort end	--jx,remove company field for certain records
	from name n, name_address na
	where na.id = @id
	  and na.address_num = @addressid
	  and n.id = na.id 
	  and na.purpose = 'MAIN'
 	if @@error <> 0
		select @err = @err + 1

	--jx
	if exists (select * from name n, name_address na
			where na.id = @id
			  and na.address_num = @addressid
			  and n.id = na.id 
			  and na.purpose = 'MAIN')
	  and @company <> (select company from name
					where id = @id)

	insert name_log
	(date_time, log_type, sub_type, user_id, id, log_text)
	select getdate(), 'Change', 'Change', 'NCOA', @id, 
	'Name.Company: ' + @company + ' -> ' + company
	from name
	where id = @id
 	if @@error <> 0
		select @err = @err + 1

  if @err <> 0
	begin
		select @id, @addressid
		ROLLBACK TRAN
	end
  else
	begin
		COMMIT TRAN
-- 		select @cnt = @cnt + 1
	end
	
	fetch next from cr_ncoa into @id, @addressid, @ncoa_mtype, @company
end

close cr_ncoa
deallocate cr_ncoa

set nocount off

-- select @total_cnt 'TOTAL TO BE PROCESSED', 
-- 	@cnt 'TOTAL PROCESSED WITH NCOA'

go



/* Look at how many are not processed */
-- select count(*) from ncoa where status = 0

/* Reset processing flag */
-- update ncoa set status = 0 where status = 1 

-- update name set company = company_sort where id = '1002534'

--select count(*) from ncoa where clientid = '1000045'