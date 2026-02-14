{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveDataTypeable #-}
module Main where

import Options.Applicative
import Language.SQL.SimpleSQL.Parse
import Language.SQL.SimpleSQL.Syntax
import Language.SQL.SimpleSQL.Pretty
import Language.SQL.SimpleSQL.Dialect
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import System.Exit
import System.IO
import Data.Generics
import Data.Maybe (fromMaybe)
import Control.Monad.Reader

data Command
  = ReplaceTable
      { file :: FilePath
      , fromTable :: String
      , toTable :: String
      , defaultCatalog :: Maybe String
      , defaultSchema :: Maybe String
      }
  | ReplaceField
      { file :: FilePath
      , fromField :: String
      , toField :: String
      , defaultCatalog :: Maybe String
      , defaultSchema :: Maybe String
      }

data Context = Context
  { currentScope :: [(T.Text, Maybe [T.Text])]
  , targetField :: [T.Text]
  , replacementField :: [T.Text]
  , defCatalog :: Maybe String
  , defSchema :: Maybe String
  } deriving (Data, Typeable)

main :: IO ()
main = do
    cmd <- execParser opts
    case cmd of
        ReplaceTable f from to dCat dSch -> handleReplaceTable f from to dCat dSch
        ReplaceField f from to dCat dSch -> handleReplaceField f from to dCat dSch
  where
    opts = info (commandParser <**> helper)
      ( fullDesc <> progDesc "Trino SQL manipulation tool" )

commandParser :: Parser Command
commandParser = subparser
    ( command "replace-table" (info replaceTableParser (progDesc "Replace a table reference"))
   <> command "replace-field" (info replaceFieldParser (progDesc "Replace a field reference"))
    )

replaceTableParser :: Parser Command
replaceTableParser = ReplaceTable
    <$> strOption (long "file" <> metavar "FILE" <> help "SQL file to process")
    <*> strOption (long "from" <> metavar "FROM" <> help "Table reference to replace (catalog.schema.table)")
    <*> strOption (long "to" <> metavar "TO" <> help "New table reference")
    <*> optional (strOption (long "default-catalog" <> help "Default catalog"))
    <*> optional (strOption (long "default-schema" <> help "Default schema"))

replaceFieldParser :: Parser Command
replaceFieldParser = ReplaceField
    <$> strOption (long "file" <> metavar "FILE" <> help "SQL file to process")
    <*> strOption (long "from" <> metavar "FROM" <> help "Field reference to replace (catalog.schema.table.field)")
    <*> strOption (long "to" <> metavar "TO" <> help "New field reference")
    <*> optional (strOption (long "default-catalog" <> help "Default catalog"))
    <*> optional (strOption (long "default-schema" <> help "Default schema"))

ansi :: Dialect
ansi = ansi2011

parseIden :: String -> [T.Text]
parseIden s = case parseScalarExpr ansi "arg" Nothing (T.pack s) of
    Right (Iden names) -> map (\(Name _ t) -> t) names
    _ -> T.splitOn "." (T.pack s)

normalize :: Maybe String -> Maybe String -> [Name] -> [T.Text]
normalize dCat dSch names =
  let texts = map (\(Name _ t) -> t) names
  in case reverse texts of
       [t] -> [fromMaybe "" (fmap T.pack dCat), fromMaybe "" (fmap T.pack dSch), t]
       [t, s] -> [fromMaybe "" (fmap T.pack dCat), s, t]
       [t, s, c] -> [c, s, t]
       parts -> reverse parts

handleReplaceTable :: FilePath -> String -> String -> Maybe String -> Maybe String -> IO ()
handleReplaceTable f from to dCat dSch = do
    sql <- TIO.readFile f
    case parseStatements ansi (T.pack f) Nothing sql of
        Left err -> do
            hPutStrLn stderr $ "Parse error: " ++ T.unpack (prettyError err)
            exitFailure
        Right ast -> do
            let fromParts = normalize dCat dSch (map (Name Nothing) (parseIden from))
            let toParts = parseIden to
            let newAst = everywhere (mkT (replaceTR fromParts toParts dCat dSch)
                                     `extT` (replaceStmtTable fromParts toParts dCat dSch)) ast
            TIO.putStrLn $ prettyStatements ansi newAst

replaceTR :: [T.Text] -> [T.Text] -> Maybe String -> Maybe String -> TableRef -> TableRef
replaceTR fromParts toParts dCat dSch (TRSimple names)
    | normalize dCat dSch names == fromParts = TRSimple (map (Name Nothing) toParts)
replaceTR _ _ _ _ tr = tr

replaceStmtTable :: [T.Text] -> [T.Text] -> Maybe String -> Maybe String -> Statement -> Statement
replaceStmtTable fromParts toParts dCat dSch (Delete names alias where_) =
    Delete (trans names) alias where_
  where trans n = if normalize dCat dSch n == fromParts then map (Name Nothing) toParts else n
replaceStmtTable fromParts toParts dCat dSch (Update names alias sets where_) =
    Update (trans names) alias sets where_
  where trans n = if normalize dCat dSch n == fromParts then map (Name Nothing) toParts else n
replaceStmtTable fromParts toParts dCat dSch (Insert names cols source) =
    Insert (trans names) cols source
  where trans n = if normalize dCat dSch n == fromParts then map (Name Nothing) toParts else n
replaceStmtTable _ _ _ _ s = s

handleReplaceField :: FilePath -> String -> String -> Maybe String -> Maybe String -> IO ()
handleReplaceField f from to dCat dSch = do
    sql <- TIO.readFile f
    case parseStatements ansi (T.pack f) Nothing sql of
        Left err -> do
            hPutStrLn stderr $ "Parse error: " ++ T.unpack (prettyError err)
            exitFailure
        Right ast -> do
            let fromParts = parseIden from
            let toParts = parseIden to
            if length fromParts /= 4 || length toParts /= 4
                then do
                    hPutStrLn stderr "From and To field references must have 4 parts (catalog.schema.table.field)"
                    exitFailure
                else do
                    let ctx = Context [] fromParts toParts dCat dSch
                    case runReaderT (mapM replaceFieldInStmt ast) ctx of
                        Left e -> do
                            hPutStrLn stderr e
                            exitFailure
                        Right newAst ->
                            TIO.putStrLn $ prettyStatements ansi newAst

type M = ReaderT Context (Either String)

extractScope :: Maybe String -> Maybe String -> TableRef -> [(T.Text, Maybe [T.Text])]
extractScope dCat dSch tr = case tr of
    TRSimple names ->
        let full = normalize dCat dSch names
            (Name _ alias) = last names
        in [(alias, Just full)]
    TRAlias inner (Alias (Name _ alias) _) ->
        let innerScope = extractScope dCat dSch inner
        in map (\(_, full) -> (alias, full)) innerScope
    TRJoin l _ _ r _ ->
        extractScope dCat dSch l ++ extractScope dCat dSch r
    TRParens inner -> extractScope dCat dSch inner
    TRQueryExpr _ -> [("", Nothing)]
    _ -> []

replaceFieldInStmt :: Statement -> M Statement
replaceFieldInStmt (SelectStatement qe) = SelectStatement <$> replaceFieldInQueryExpr qe
replaceFieldInStmt (Update ns alias sets where_) = do
    ctx <- ask
    let full = normalize (defCatalog ctx) (defSchema ctx) ns
        (Name _ name) = last ns
        aliasName = fromMaybe name (fmap (\(Name _ a) -> a) alias)
        newScope = [(aliasName, Just full)]
    let newCtx = ctx { currentScope = newScope }
    newSets <- lift $ runReaderT (mapM replaceFieldInSetClause sets) newCtx
    newWhere <- case where_ of
        Just e -> Just <$> lift (runReaderT (replaceFieldInScalar e) newCtx)
        Nothing -> return Nothing
    return $ Update ns alias newSets newWhere
replaceFieldInStmt (Delete ns alias where_) = do
    ctx <- ask
    let full = normalize (defCatalog ctx) (defSchema ctx) ns
        (Name _ name) = last ns
        aliasName = fromMaybe name (fmap (\(Name _ a) -> a) alias)
        newScope = [(aliasName, Just full)]
    let newCtx = ctx { currentScope = newScope }
    newWhere <- case where_ of
        Just e -> Just <$> lift (runReaderT (replaceFieldInScalar e) newCtx)
        Nothing -> return Nothing
    return $ Delete ns alias newWhere
replaceFieldInStmt (Insert ns cols source) = do
    ctx <- ask
    let full = normalize (defCatalog ctx) (defSchema ctx) ns
        targetTName = init (targetField ctx)
        isTargetTable = full == targetTName
    let newCols = if isTargetTable
                  then fmap (map (\n@(Name _ t) -> if t == last (targetField ctx)
                                                   then Name Nothing (last (replacementField ctx))
                                                   else n)) cols
                  else cols
    newSource <- replaceFieldInInsertSource source
    return $ Insert ns newCols newSource
replaceFieldInStmt s = return s

replaceFieldInInsertSource :: InsertSource -> M InsertSource
replaceFieldInInsertSource (InsertQuery qe) = InsertQuery <$> replaceFieldInQueryExpr qe
replaceFieldInInsertSource DefaultInsertValues = return DefaultInsertValues

replaceFieldInSetClause :: SetClause -> M SetClause
replaceFieldInSetClause (Set ns e) = do
    ctx <- ask
    newE <- replaceFieldInScalar e
    let fName = last (map (\(Name _ t) -> t) ns)
        targetFName = last (targetField ctx)
        targetTName = init (targetField ctx)
        isTargetTable = any (\(_, full) -> full == Just targetTName) (currentScope ctx)
        newNs = if isTargetTable && fName == targetFName
                then init ns ++ [Name Nothing (last (replacementField ctx))]
                else ns
    return $ Set newNs newE
replaceFieldInSetClause sc = return sc

replaceFieldInQueryExpr :: QueryExpr -> M QueryExpr
replaceFieldInQueryExpr qe@(Select {}) = do
    ctx <- ask
    newScope <- concat <$> mapM (return . extractScope (defCatalog ctx) (defSchema ctx)) (qeFrom qe)
    let newCtx = ctx { currentScope = newScope }
    newSL <- lift $ runReaderT (mapM (\(e, n) -> (,) <$> replaceFieldInScalar e <*> pure n) (qeSelectList qe)) newCtx
    newF <- lift $ runReaderT (mapM replaceFieldInTableRef (qeFrom qe)) newCtx
    newW <- case qeWhere qe of
        Just e -> Just <$> lift (runReaderT (replaceFieldInScalar e) newCtx)
        Nothing -> return Nothing
    newG <- lift $ runReaderT (mapM replaceFieldInGroupingExpr (qeGroupBy qe)) newCtx
    newH <- case qeHaving qe of
        Just e -> Just <$> lift (runReaderT (replaceFieldInScalar e) newCtx)
        Nothing -> return Nothing
    newO <- lift $ runReaderT (mapM replaceFieldInSortSpec (qeOrderBy qe)) newCtx
    return $ qe { qeSelectList = newSL, qeFrom = newF, qeWhere = newW, qeGroupBy = newG, qeHaving = newH, qeOrderBy = newO }
replaceFieldInQueryExpr (QueryExprParens qe) = QueryExprParens <$> replaceFieldInQueryExpr qe
replaceFieldInQueryExpr (Values vss) = Values <$> mapM (mapM replaceFieldInScalar) vss
replaceFieldInQueryExpr q = return q

replaceFieldInGroupingExpr :: GroupingExpr -> M GroupingExpr
replaceFieldInGroupingExpr (SimpleGroup e) = SimpleGroup <$> replaceFieldInScalar e
replaceFieldInGroupingExpr g = return g

replaceFieldInSortSpec :: SortSpec -> M SortSpec
replaceFieldInSortSpec (SortSpec e dir nulls) = do
    newE <- replaceFieldInScalar e
    return $ SortSpec newE dir nulls

replaceFieldInTableRef :: TableRef -> M TableRef
replaceFieldInTableRef (TRQueryExpr qe) = TRQueryExpr <$> replaceFieldInQueryExpr qe
replaceFieldInTableRef (TRJoin l b j r c) = do
    newL <- replaceFieldInTableRef l
    newR <- replaceFieldInTableRef r
    newC <- case c of
        Just (JoinOn e) -> Just . JoinOn <$> replaceFieldInScalar e
        Just (JoinUsing ns) -> return $ Just (JoinUsing ns)
        Nothing -> return Nothing
    return $ TRJoin newL b j newR newC
replaceFieldInTableRef (TRParens tr) = TRParens <$> replaceFieldInTableRef tr
replaceFieldInTableRef (TRAlias tr a) = TRAlias <$> replaceFieldInTableRef tr <*> pure a
replaceFieldInTableRef tr = return tr

replaceFieldInScalar :: ScalarExpr -> M ScalarExpr
replaceFieldInScalar (Iden names) = do
    ctx <- ask
    let fName = last (map (\(Name _ t) -> t) names)
        targetFName = last (targetField ctx)
        targetTName = init (targetField ctx)
    if fName == targetFName
       then case names of
         [_] -> do -- unqualified
           let matches = filter (\(_, full) -> full == Just targetTName) (currentScope ctx)
           if null matches
              then return (Iden names)
              else if length (currentScope ctx) > 1
                   then lift $ Left "ambiguous references were found and adjusted SQL with qualified references is necessary"
                   else return $ Iden [Name Nothing (last (replacementField ctx))]
         _ -> do -- qualified
           let qualNames = init names
               qualFull = normalize (defCatalog ctx) (defSchema ctx) qualNames
               isAliasMatch = case qualNames of
                 [Name _ a] -> any (\(alias, full) -> a == alias && full == Just targetTName) (currentScope ctx)
                 _ -> False
               isFullMatch = qualFull == targetTName
           if isAliasMatch || isFullMatch
              then return $ Iden (init names ++ [Name Nothing (last (replacementField ctx))])
              else return (Iden names)
       else return (Iden names)
replaceFieldInScalar (BinOp l ns r) = BinOp <$> replaceFieldInScalar l <*> pure ns <*> replaceFieldInScalar r
replaceFieldInScalar (PrefixOp ns e) = PrefixOp ns <$> replaceFieldInScalar e
replaceFieldInScalar (PostfixOp ns e) = PostfixOp ns <$> replaceFieldInScalar e
replaceFieldInScalar (App ns args) = App ns <$> mapM replaceFieldInScalar args
replaceFieldInScalar (Parens e) = Parens <$> replaceFieldInScalar e
replaceFieldInScalar (Cast e tn) = Cast <$> replaceFieldInScalar e <*> pure tn
replaceFieldInScalar (Case t ws e) = Case <$> mapM replaceFieldInScalar t
                                         <*> mapM (\(w, res) -> (,) <$> mapM replaceFieldInScalar w <*> replaceFieldInScalar res) ws
                                         <*> mapM replaceFieldInScalar e
replaceFieldInScalar (In b e list) = In b <$> replaceFieldInScalar e <*> replaceFieldInInList list
  where replaceFieldInInList (InList exprs) = InList <$> mapM replaceFieldInScalar exprs
        replaceFieldInInList (InQueryExpr qe) = InQueryExpr <$> replaceFieldInQueryExpr qe
replaceFieldInScalar (SubQueryExpr typ qe) = SubQueryExpr typ <$> replaceFieldInQueryExpr qe
replaceFieldInScalar e = return e
