-- Model Dimension

drop table if exists VehicleSales.Gold.Model

create table VehicleSales.Gold.Model
as
select 
ROW_NUMBER() over(order by (select null)) as ModelID,
d.*
from (
        select distinct Make, Model, Trim, Body
        from VehicleSales.Silver.CarPrices
    ) as d


-- Seller Dimension
drop table if exists VehicleSales.Gold.Seller

create table VehicleSales.Gold.Seller
as
select 
ROW_NUMBER() over(order by (select null)) as SellerID,
d.*
from
    (select 
    distinct Seller
    from VehicleSales.Silver.CarPrices) as d


-- Color Dimension
drop table if exists VehicleSales.Gold.Color

create table VehicleSales.Gold.Color
as
select 
ROW_NUMBER() over(order by (select null)) as ColorID,
d.*
from
    (select distinct BodyColor, InteriorColor
    from VehicleSales.Silver.CarPrices) as d


-- Transmission Dimension
drop table if exists VehicleSales.Gold.TransmissionType

create table VehicleSales.Gold.TransmissionType
as
select 
ROW_NUMBER() over(order by (select null)) as TransmissionTypeID,
d.*
from
    (select 
    distinct Transmission
    from VehicleSales.Silver.CarPrices) as d