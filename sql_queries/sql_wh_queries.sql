create SCHEMA Silver

/*SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH 
FROM BronzeLayer.INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'car_prices';*/

alter proc Silver.CarPricesLoad
as
begin

    drop table if exists Silver.CarPrices

    create table Silver.CarPrices
    as 
    select
        try_cast(year as int) as ModelYear,
        cast(lower(make) as VARCHAR(8000)) as Make,
        cast(lower(model) as VARCHAR(8000)) as Model,
        cast(lower(trim) as VARCHAR(8000)) as Trim,
        cast(lower(body) as VARCHAR(8000)) as Body,
        cast(lower(transmission) as VARCHAR(8000)) as Transmission,
        cast(lower(vin) as VARCHAR(8000)) as VIN,
        cast(lower(state) as VARCHAR(8000)) as SalesState,
        case when condition <5 then try_cast(condition as decimal(4,1)) else try_cast(condition as decimal(4,1)) / 10.0 end as Condition,
        try_cast(odometer as int) as Miles,
        cast(lower(color) as VARCHAR(8000)) as BodyColor,
        cast(lower(interior) as VARCHAR(8000)) as InteriorColor,
        cast(lower(seller) as VARCHAR(8000)) as Seller,
        try_cast(mmr as int) as ManheimMarketReportValue,
        try_cast(sellingprice as int) as SellingPrice,
        --try_cast(format(try_cast(substring(saledate,5, 20) as date), 'yyyyMMdd') as int) as SalesDate,
        --format(try_cast(substring(saledate,5, 20) as datetime2), 'hhmm') as SalesTime
        --above format() function produces varchar(4000) error
        TRY_CAST(CONVERT(VARCHAR(8), TRY_CAST(SUBSTRING(saledate,5,20) AS DATE), 112) AS INT) AS SalesDate,
        RIGHT('0' + CONVERT(VARCHAR(2), DATEPART(HOUR, TRY_CAST(SUBSTRING(saledate,5,20) AS DATETIME2))), 2) +
        RIGHT('0' + CONVERT(VARCHAR(2), DATEPART(MINUTE, TRY_CAST(SUBSTRING(saledate,5,20) AS DATETIME2))), 2) AS SalesTime
    FROM BronzeLayer.dbo.car_prices
    where isnumeric(color) <> 1 -- making sure VW wrong rows have been fixed

end

-- exec Silver.CarPricesLoad


