from pydantic import BaseModel, EmailStr, Field
from typing import List, Optional, Annotated
from datetime import datetime
from decimal import Decimal
from pydantic import condecimal

# Type aliases for decimal fields
PriceDecimal = Annotated[Decimal, condecimal(max_digits=10, decimal_places=2)]

# Track schemas
class TrackOut(BaseModel):
    TrackId: int
    Name: str
    AlbumId: Optional[int] = None
    MediaTypeId: int
    GenreId: Optional[int] = None
    Composer: Optional[str] = None
    Milliseconds: int
    Bytes: Optional[int] = None
    UnitPrice: PriceDecimal

    class Config:
        from_attributes = True


# Customer schemas
class CustomerCreate(BaseModel):
    FirstName: str = Field(..., min_length=1, max_length=40)
    LastName: str = Field(..., min_length=1, max_length=20)
    Email: EmailStr = Field(..., max_length=60)


class CustomerOut(BaseModel):
    CustomerId: int
    FirstName: str
    LastName: str
    Email: str

    class Config:
        from_attributes = True


class CustomerResponse(BaseModel):
    CustomerId: int
    message: str = "Customer created successfully"


# Cart and Checkout schemas
class CartItem(BaseModel):
    TrackId: int = Field(..., gt=0)
    Quantity: int = Field(..., gt=0, le=100)
    UnitPrice: PriceDecimal = Field(..., gt=0)


class CheckoutIn(BaseModel):
    CustomerId: int = Field(..., gt=0)
    BillingAddress: Optional[str] = Field(None, max_length=70)
    BillingCity: Optional[str] = Field(None, max_length=40)
    BillingCountry: Optional[str] = Field(None, max_length=40)
    Items: List[CartItem] = Field(..., min_items=1)


# Invoice schemas
class InvoiceLineOut(BaseModel):
    InvoiceLineId: int
    TrackId: int
    UnitPrice: PriceDecimal
    Quantity: int

    class Config:
        from_attributes = True


class InvoiceOut(BaseModel):
    InvoiceId: int
    CustomerId: int
    InvoiceDate: datetime
    BillingAddress: Optional[str] = None
    BillingCity: Optional[str] = None
    BillingCountry: Optional[str] = None
    Total: PriceDecimal
    Lines: List[InvoiceLineOut] = []

    class Config:
        from_attributes = True


class CheckoutResponse(BaseModel):
    InvoiceId: int
    Total: str
    message: str = "Checkout completed successfully"


# Additional schemas for complete API
class AlbumOut(BaseModel):
    AlbumId: int
    Title: str
    ArtistId: int

    class Config:
        from_attributes = True


class ArtistOut(BaseModel):
    ArtistId: int
    Name: str

    class Config:
        from_attributes = True


class GenreOut(BaseModel):
    GenreId: int
    Name: str

    class Config:
        from_attributes = True


class MediaTypeOut(BaseModel):
    MediaTypeId: int
    Name: str

    class Config:
        from_attributes = True


# Search and filter schemas
class TrackSearchParams(BaseModel):
    name: Optional[str] = None
    album_id: Optional[int] = None
    genre_id: Optional[int] = None
    min_price: Optional[Decimal] = None
    max_price: Optional[Decimal] = None
    limit: int = Field(default=50, ge=1, le=100)
    offset: int = Field(default=0, ge=0)
