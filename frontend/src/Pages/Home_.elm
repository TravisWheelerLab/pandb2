module Pages.Home_ exposing (Model, Msg, page)

import Api
import Dict exposing (Dict)
import Effect exposing (Effect)
import FormatNumber
import FormatNumber.Locales exposing (Decimals(..), usLocale)
import Html
import Html.Attributes exposing (attribute, class, height, href, selected, src)
import Html.Events exposing (onInput)
import Http
import Json.Decode as Decode exposing (Decoder, int, list, nullable, string)
import Json.Decode.Pipeline exposing (optional, required)
import List.Extra
import Page exposing (Page)
import Route exposing (Route)
import Shared
import View exposing (View)


page : Shared.Model -> Route () -> Page Model Msg
page shared route =
    Page.new
        { init = init
        , update = update shared
        , subscriptions = subscriptions
        , view = view
        }



-- INIT


validLinkFields =
    [ "hml_id", "mindcrowd_id" ]


type alias Model =
    { tables : List String
    , dataDictionary : List DataDictionary
    , errorMessage : Maybe String
    , primaryTable : String
    , linkOptions : List String
    , primaryTableLink : Maybe String
    , additionalTableOptions : List String
    , additionalTables : List String
    , data : List Data
    , dataColumns : List String
    , recordCount : Maybe Int
    , pageSize : Int
    , pageNumber : Int
    }


initialModel : Model
initialModel =
    { tables = []
    , dataDictionary = []
    , errorMessage = Nothing
    , primaryTable = ""
    , linkOptions = []
    , primaryTableLink = Nothing
    , additionalTableOptions = []
    , additionalTables = []
    , data = []
    , dataColumns = []
    , recordCount = Nothing
    , pageSize = 25
    , pageNumber = 1
    }


type alias DataDictionary =
    { tableName : String
    , alias : String
    , fieldLabel : String
    , definition : Maybe String
    }


type ValueType
    = StringValue String
    | BoolValue Bool
    | FloatValue Float
    | IntValue Int


decoderValueType : Decoder ValueType
decoderValueType =
    Decode.oneOf
        [ Decode.map StringValue Decode.string
        , Decode.map BoolValue Decode.bool
        , Decode.map FloatValue Decode.float
        , Decode.map IntValue Decode.int
        ]


type alias Data =
    Dict.Dict String ValueType


type alias DataResult =
    { results : List Data
    , count : Int
    , columns : List String
    }


decodeDataResult : Decoder DataResult
decodeDataResult =
    Decode.succeed DataResult
        |> required "results" (list decodeData)
        |> required "count" int
        |> required "columns" (list string)


decodeData : Decoder Data
decodeData =
    Decode.dict decoderValueType


decodeDataDictionary : Decoder DataDictionary
decodeDataDictionary =
    Decode.succeed DataDictionary
        |> required "table_name" string
        |> required "alias" string
        |> required "field_label" string
        |> optional "definition" (nullable string) Nothing


init : () -> ( Model, Effect Msg )
init () =
    ( initialModel
    , Effect.sendMsg RequestDataDictionary
    )



-- UPDATE


type Msg
    = AddOtherTable String
    | GotData (Result Http.Error DataResult)
    | GotDataDictionary (Result Http.Error (List DataDictionary))
    | RequestData
    | RequestDataDictionary
    | SetErrorMessage (Maybe String)
    | SetPrimaryTable String
    | SetPrimaryTableLink String
    | UpdatePageNumber String
    | UpdatePageSize String


update : Shared.Model -> Msg -> Model -> ( Model, Effect Msg )
update shared msg model =
    case msg of
        AddOtherTable tableName ->
            ( { model
                | additionalTables =
                    List.Extra.unique <|
                        List.append model.additionalTables [ tableName ]
              }
            , Effect.none
            )

        GotData (Ok result) ->
            ( { model
                | data = result.results
                , dataColumns = result.columns
                , recordCount = Just result.count
              }
            , Effect.none
            )

        GotData (Err err) ->
            ( { model
                | data = []
                , dataColumns = []
                , recordCount = Nothing
                , errorMessage = Just (Api.toUserFriendlyMessage err)
              }
            , Effect.none
            )

        GotDataDictionary (Ok result) ->
            let
                dataDictionary =
                    List.filter
                        (\dd -> List.member dd.fieldLabel validLinkFields)
                        result

                tables =
                    List.map .alias dataDictionary
                        |> List.Extra.unique
                        |> List.sort
            in
            ( { model
                | dataDictionary = dataDictionary
                , tables = tables
                , errorMessage = Nothing
              }
            , Effect.sendMsg (SetPrimaryTable "MRI Anatomic Measures")
            )

        GotDataDictionary (Err err) ->
            ( { model
                | dataDictionary = []
                , errorMessage = Just (Api.toUserFriendlyMessage err)
              }
            , Effect.none
            )

        RequestData ->
            ( model, requestData shared model )

        RequestDataDictionary ->
            ( model, requestDataDictionary shared model )

        SetErrorMessage newErrorMessage ->
            ( { model | errorMessage = newErrorMessage }, Effect.none )

        SetPrimaryTable newMainTable ->
            let
                linkOptions =
                    List.filter
                        (\dd ->
                            dd.alias
                                == newMainTable
                                && List.member dd.fieldLabel validLinkFields
                        )
                        model.dataDictionary
                        |> List.map .fieldLabel

                ( additionalTableOptions, primaryTableLink ) =
                    case linkOptions of
                        -- If there's just one linkField,
                        -- go ahead and find the other tables
                        [ linkField ] ->
                            ( List.filter
                                (\dd ->
                                    dd.alias
                                        /= newMainTable
                                        && dd.fieldLabel
                                        == linkField
                                )
                                model.dataDictionary
                                |> List.map .alias
                                |> List.sort
                            , Just linkField
                            )

                        _ ->
                            ( [], Nothing )

                additionalTables =
                    case additionalTableOptions of
                        [ tableName ] ->
                            [ tableName ]

                        _ ->
                            []
            in
            ( { model
                | primaryTable = newMainTable
                , primaryTableLink = primaryTableLink
                , linkOptions = linkOptions
                , additionalTableOptions = additionalTableOptions
                , additionalTables = additionalTables
              }
            , Effect.sendMsg RequestData
            )

        SetPrimaryTableLink linkField ->
            let
                additionalTableOptions =
                    List.filter
                        (\dd ->
                            dd.alias
                                /= model.primaryTable
                                && dd.fieldLabel
                                == linkField
                        )
                        model.dataDictionary
                        |> List.map .alias
                        |> List.sort

                additionalTables =
                    case additionalTableOptions of
                        [ tableName ] ->
                            [ tableName ]

                        _ ->
                            []
            in
            ( { model
                | primaryTableLink = Just linkField
                , additionalTableOptions = additionalTableOptions
                , additionalTables = additionalTables
              }
            , Effect.sendMsg RequestData
            )

        UpdatePageSize newSize ->
            case String.toInt newSize of
                Just size ->
                    let
                        newModel =
                            { model
                                | pageSize = size
                                , pageNumber = 1
                            }
                    in
                    ( newModel
                    , Effect.sendMsg RequestData
                    )

                Nothing ->
                    ( { model | errorMessage = Just "Invalid page size" }
                    , Effect.none
                    )

        UpdatePageNumber newNumber ->
            case String.toInt newNumber of
                Just pageNumber ->
                    let
                        newModel =
                            { model | pageNumber = pageNumber }
                    in
                    ( newModel
                    , Effect.sendMsg RequestData
                    )

                Nothing ->
                    ( { model | errorMessage = Just "Invalid page number" }
                    , Effect.none
                    )


requestData : Shared.Model -> Model -> Effect Msg
requestData shared model =
    let
        primaryTable =
            List.filter
                (\dd -> dd.alias == model.primaryTable)
                model.dataDictionary
                |> List.map .tableName
                |> List.head

        offset =
            if model.pageNumber > 1 then
                "&offset="
                    ++ String.fromInt
                        ((model.pageNumber
                            - 1
                         )
                            * model.pageSize
                        )

            else
                ""

        limit =
            "&limit="
                ++ String.fromInt model.pageSize
    in
    case primaryTable of
        Just tableName ->
            Effect.sendCmd <|
                Http.get
                    { url =
                        shared.apiHost
                            ++ "/get_data"
                            ++ "?primary_table="
                            ++ tableName
                            ++ offset
                            ++ limit
                    , expect = Http.expectJson GotData decodeDataResult
                    }

        _ ->
            Effect.sendMsg (SetErrorMessage (Just "No primary table name"))


requestDataDictionary : Shared.Model -> Model -> Effect Msg
requestDataDictionary shared model =
    Effect.sendCmd <|
        Http.get
            { url = shared.apiHost ++ "/get_data_dictionary"
            , expect = Http.expectJson GotDataDictionary (list decodeDataDictionary)
            }



-- SUBSCRIPTIONS


subscriptions : Model -> Sub Msg
subscriptions model =
    Sub.none



-- VIEW


view : Model -> View Msg
view model =
    let
        blank =
            Html.option [] [ Html.text "-- Select --" ]

        recordCount =
            Maybe.withDefault 0 model.recordCount

        numPages =
            recordCount // model.pageSize

        pageNav =
            let
                options =
                    List.map
                        (\num ->
                            Html.option [ selected (num == model.pageNumber) ]
                                [ Html.text (String.fromInt num) ]
                        )
                        (List.range
                            1
                            (numPages + 1)
                        )
            in
            Html.div [ class "control" ]
                [ Html.p [ class "heading" ] [ Html.text "Page:" ]
                , Html.div []
                    [ Html.select [ class "select", onInput UpdatePageNumber ] options ]
                ]

        pageSizeNav =
            let
                options =
                    List.map
                        (\num ->
                            Html.option [ selected (model.pageSize == num) ]
                                [ Html.text (String.fromInt num) ]
                        )
                        [ 10, 25, 50, 100 ]
            in
            Html.div [ class "control" ]
                [ Html.p [ class "heading" ] [ Html.text "Show:" ]
                , Html.div []
                    [ Html.select [ class "select", onInput UpdatePageSize ] options
                    ]
                ]

        tables =
            let
                mkOption value =
                    Html.option
                        [ selected (value == model.primaryTable) ]
                        [ Html.text value ]

                tableSelect =
                    Html.select
                        [ class "select", onInput SetPrimaryTable ]
                    <|
                        List.concat
                            [ [ blank ]
                            , List.map mkOption model.tables
                            ]
            in
            if List.isEmpty model.tables then
                Html.text "No tables"

            else
                Html.div [ class "field" ]
                    [ Html.label
                        [ class "label" ]
                        [ Html.text "Primary Table:" ]
                    , Html.div [ class "control" ] [ tableSelect ]
                    ]

        links =
            let
                mkOption value =
                    Html.option [] [ Html.text value ]

                linkSelect =
                    case List.length model.linkOptions of
                        0 ->
                            Html.text "No links"

                        1 ->
                            Html.text (Maybe.withDefault "" (List.head model.linkOptions))

                        _ ->
                            Html.select [ class "select", onInput SetPrimaryTableLink ] <|
                                List.concat
                                    [ [ blank ]
                                    , List.map mkOption model.linkOptions
                                    ]
            in
            Html.div [ class "field" ]
                [ Html.label
                    [ class "label" ]
                    [ Html.text "Join additional tables on:" ]
                , Html.div [ class "control" ] [ linkSelect ]
                ]

        additionalTableOptions =
            let
                mkOption value =
                    Html.option [] [ Html.text value ]

                otherSelect =
                    if List.isEmpty model.additionalTableOptions then
                        Html.text "No other tables"

                    else
                        Html.select [ class "select", onInput AddOtherTable ] <|
                            List.concat
                                [ [ blank ]
                                , List.map mkOption model.additionalTableOptions
                                ]
            in
            Html.div [ class "field" ]
                [ Html.label
                    [ class "label" ]
                    [ Html.text "Additional tables:" ]
                , Html.div [ class "control" ] [ otherSelect ]
                ]

        numFound =
            Html.div []
                [ Html.p [ class "heading" ] [ Html.text "Found:" ]
                , Html.p [ class "title" ] [ Html.text (commify recordCount) ]
                ]

        pagination =
            Html.div [ class "container" ]
                [ Html.nav [ class "level" ]
                    [ Html.div [ class "level-item has-text-centered" ] [ numFound ]
                    , Html.div [ class "level-item has-text-centered" ] [ pageSizeNav ]
                    , Html.div [ class "level-item has-text-centered" ] [ pageNav ]
                    ]
                ]

        errorMessage =
            case model.errorMessage of
                Just error ->
                    Html.section
                        [ class "hero is-warning is-small" ]
                        [ Html.div [ class "hero-body" ]
                            [ Html.p [ class "title" ] [ Html.text "Error" ]
                            , Html.p [ class "subtitle" ] [ Html.text error ]
                            ]
                        ]

                _ ->
                    Html.div [] []

        navbar =
            Html.div [ class "container" ]
                [ Html.nav
                    [ class "navbar"
                    , attribute "role" "navigation"
                    , attribute "aria-label" "main navigation"
                    ]
                    [ Html.div
                        [ class "navbar-brand" ]
                        [ Html.a
                            [ href "/" ]
                            [ Html.img [ src "/assets/pan.png", height 60 ] [] ]
                        ]
                    , Html.div [ class "navbar-menu" ]
                        [ Html.div [ class "navbar-end" ]
                            [ Html.button
                                [ href "mailto:help@pandb.org", class "button is-info" ]
                                [ Html.text "Email Support" ]
                            ]
                        ]
                    ]
                ]

        tableOptions =
            Html.div [ class "columns" ]
                [ Html.div [ class "column" ] [ tables ]
                , Html.div [ class "column" ] [ links ]
                , Html.div [ class "column" ] [ additionalTableOptions ]
                ]

        {-
           info =
               Html.div []
                   [ Html.div []
                       [ Html.text <|
                           "Primary table: "
                               ++ model.primaryTable
                       ]
                   , Html.div []
                       [ Html.text <|
                           "Primary table link: "
                               ++ Maybe.withDefault "None" model.primaryTableLink
                       ]
                   , Html.div []
                       [ Html.text <|
                           "Additional tables: "
                               ++ String.join ", " model.additionalTables
                       ]
                   ]
        -}
    in
    { title = "PANdb"
    , body =
        [ navbar
        , errorMessage
        , Html.div [ class "container" ]
            [ tableOptions
            , pagination
            , dataTable model.dataColumns model.data
            ]
        ]
    }


dataTable : List String -> List Data -> Html.Html msg
dataTable columnNames data =
    let
        viewValue value =
            case value of
                BoolValue val ->
                    if val then
                        "True"

                    else
                        "False"

                IntValue val ->
                    String.fromInt val

                FloatValue val ->
                    String.fromFloat val

                StringValue val ->
                    val

        mkRow rec =
            Html.tr []
                (List.map
                    (\val -> Html.td [] [ Html.text (viewValue val) ])
                    (Dict.values rec)
                )

        mkRow2 rec =
            Html.tr []
                (List.map
                    (\col ->
                        Html.td []
                            [ Html.text <|
                                viewValue <|
                                    Maybe.withDefault (StringValue "NA")
                                        (Dict.get col rec)
                            ]
                    )
                    columnNames
                )
    in
    if List.isEmpty data then
        Html.text "No data"

    else
        Html.div [ class "table-container" ]
            [ Html.table [ class "table" ]
                [ Html.thead []
                    (List.map (\col -> Html.th [] [ Html.text col ]) columnNames)
                , Html.tbody [] (List.map mkRow data)
                ]
            ]


commify : Int -> String
commify num =
    let
        locale =
            { usLocale
                | decimals = Exact 0
            }
    in
    FormatNumber.format locale (toFloat num)
