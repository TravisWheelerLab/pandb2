module Shared exposing
    ( Flags, decoder
    , Model, Msg
    , init, update, subscriptions
    )

{-|

@docs Flags, decoder
@docs Model, Msg
@docs init, update, subscriptions

-}

import Effect exposing (Effect)
import Json.Decode exposing (Decoder, string)
import Json.Decode.Pipeline exposing (optional)
import Route exposing (Route)
import Route.Path
import Shared.Model
import Shared.Msg



-- FLAGS


type alias Flags =
    { apiHost : String }


defaultApiHost =
    "http://localhost"


decoder : Json.Decode.Decoder Flags
decoder =
    Json.Decode.succeed Flags
        |> optional "API_HOST" string defaultApiHost



-- INIT


type alias Model =
    Shared.Model.Model


init : Result Json.Decode.Error Flags -> Route () -> ( Model, Effect Msg )
init flagsResult route =
    let
        apiHost =
            case flagsResult of
                Ok flags ->
                    flags.apiHost

                _ ->
                    defaultApiHost
    in
    ( { apiHost = apiHost ++ "/api/v1" }
    , Effect.none
    )



-- UPDATE


type alias Msg =
    Shared.Msg.Msg


update : Route () -> Msg -> Model -> ( Model, Effect Msg )
update route msg model =
    case msg of
        Shared.Msg.NoOp ->
            ( model
            , Effect.none
            )



-- SUBSCRIPTIONS


subscriptions : Route () -> Model -> Sub Msg
subscriptions route model =
    Sub.none
