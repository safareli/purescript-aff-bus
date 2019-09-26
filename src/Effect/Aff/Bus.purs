{-
Copyright 2018 SlamData, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-}

module Effect.Aff.Bus
  ( make
  , read
  , consume
  , consumeLatest
  , Step(..)
  , write
  , split
  , kill
  , isKilled
  , Cap
  , Bus
  , BusRW
  , BusR
  , BusR'
  , BusW
  , BusW'
  ) where

import Prelude

import Control.Lazy (fix)
import Control.Monad.Reader (ReaderT, ask, lift, runReaderT)
import Control.Monad.Rec.Class (forever)
import Data.Either (Either(..))
import Data.Foldable (for_)
import Data.List (List, catMaybes, (:))
import Data.Maybe (Maybe(..))
import Data.Traversable (for)
import Data.Tuple (Tuple(..))
import Effect (Effect)
import Effect.AVar as EffAVar
import Effect.Aff (Aff, Error, generalBracket, killFiber, launchAff, launchAff_, try)
import Effect.Aff.AVar (AVar)
import Effect.Aff.AVar as AVar
import Effect.Aff.Finally as F
import Effect.Class (class MonadEffect, liftEffect)
import Effect.Exception as Exn
import Effect.Ref (Ref)
import Effect.Ref as Ref
import Unsafe.Coerce (unsafeCoerce)

data Cap

data Bus (r :: # Type) a = Bus (AVar (Either Error a)) (AVar (List (Ref (Maybe (Consumer a)))))



foreign import data Consumer :: Type -> Type

type ConsumerR output input =
  { outputVar :: AVar output
  , consumer :: input -> ReaderT (AVar output) Effect Unit
  }

mkConsumer :: ∀ output input. ConsumerR output input -> Consumer input
mkConsumer = unsafeCoerce

runConsumer :: ∀ input r. (∀ output. ConsumerR output input -> r) -> Consumer input -> r
runConsumer = unsafeCoerce


type BusR = BusR' ()

type BusR' r = Bus (read :: Cap | r)

type BusW = BusW' ()

type BusW' r = Bus (write :: Cap | r)

type BusRW = Bus (read :: Cap, write :: Cap)

-- | Creates a new bidirectional Bus which can be read from and written to.
make :: ∀ m a. MonadEffect m ⇒ m (BusRW a)
make = liftEffect do
  cell ← EffAVar.empty
  consumers ← EffAVar.new mempty
  launchAff_ $ fix \loop -> do
    -- we `read` from `cell` instead of `take`, so that if error is written,
    -- `cell` can be killed, such that if there was any other `put` operations
    -- blocked, that will resolve with the error.
    resE ← AVar.read cell
    case resE of
      Left err -> do
        pure unit
        cs ← AVar.take consumers
        liftEffect do
          for_ cs $ \cRef -> do
            mbC <- liftEffect $ Ref.read cRef
            for_ mbC (runConsumer (_.outputVar >>> EffAVar.kill err))
          EffAVar.kill err consumers
          EffAVar.kill err cell
      Right res -> do
        void $ AVar.take cell
        cs ← AVar.take consumers
        consumersMb <- for cs $ \cRef -> do
          mbC <- liftEffect $ Ref.read cRef
          case mbC of
            Nothing -> pure Nothing
            Just c -> c # runConsumer \{outputVar, consumer} -> do
              try (liftEffect (runReaderT (consumer res) outputVar)) >>= case _ of
                Right _ -> do
                  pure $ Just cRef
                Left err -> do
                  AVar.kill err outputVar
                  pure $ Nothing
        AVar.put (catMaybes consumersMb) consumers
        loop
  pure $ Bus cell consumers

-- | Blocks until a new value is pushed to the Bus, returning the value.
read :: ∀ a r. BusR' r a -> Aff a
read bus = consume bus $ \a -> ask >>= AVar.put a >>> launchAff_ >>> lift

data Step res = Loop | Done res

-- | Registers a new consumer on the bus. Blocks until the consumer returns
-- | `Done x` and then computation is resolved with the `x`. 
-- | if Effect raises an exception it will be propagated
consume :: ∀ a r output. BusR' r a -> (a -> ReaderT (AVar output) Effect Unit) -> Aff output
consume (Bus _ consumers) consumer = F.runFinallyM do
  cs ← EffAVar.take consumers # F.finally' do
    -- If we were able to `take` we must put it back.
    F.onResult \cs -> lift $ AVar.put cs consumers

  {outputVar, cRef} <- lift $ liftEffect do
    outputVar <- EffAVar.empty
    cRef <- Ref.new $ Just $ mkConsumer {outputVar, consumer}
    void $ EffAVar.read outputVar \_ -> do
      -- after outputVar is killed or filled we clear
      -- the ref to avoid any possible memory leak.
      Ref.write Nothing cRef
    pure {outputVar, cRef}
      

  EffAVar.put (cRef : cs) consumers # F.finally' do
    -- If we were able to `put` then we must kill outputVar
    F.onResult \_ -> (F.onError <> F.onFinalError) \err -> do
      lift $ AVar.kill err outputVar
    -- Otherwise we should `put` back to `consumers`.
    F.onError \_ -> lift $ AVar.put cs consumers

  EffAVar.take outputVar # F.finally' do
    -- here in any case we kill `outputVar`
    lift $ AVar.kill (Exn.error "cleanup") outputVar

-- | Same as `consume` but consuming function returns Aff instead of Effect.
-- | When consuming function is not completed and new value comes in, it gets
-- | killed and is invoked with new value.
consumeLatest :: ∀ a r output. BusR' r a -> (a -> Aff (Step output)) -> Aff output
consumeLatest bus consumer = do
  fiberRef <- liftEffect $ Ref.new (pure unit)
  consume bus \val -> do
    outputVar <- ask
    lift $ Ref.read fiberRef >>= (killFiber (Exn.error "kill from consumeLatest") >>> launchAff_)
    fiber <- lift $ launchAff do
      void $ generalBracket (pure unit)
        { killed: const pure
        , failed: \err _ -> AVar.kill err outputVar
        , completed: \cs _ -> case cs of
            Loop -> pure unit
            Done res -> AVar.put res outputVar
        }
        (const $ consumer val)
    lift $ Ref.write fiber fiberRef

-- | Pushes a new value to the Bus, yielding immediately.
write :: ∀ a r. a -> BusW' r a -> Aff Unit
write a (Bus cell _) = AVar.put (Right a) cell

-- | Splits a bidirectional Bus into separate read and write Buses.
split :: ∀ a. BusRW a -> Tuple (BusR a) (BusW a)
split (Bus a b) = Tuple (Bus a b) (Bus a b)

-- | Kills the Bus and propagates the exception to all pending and future consumers.
-- | `kill` is idempotent and blocks until killing process is fully finishes, i.e.
-- | `kill err bus *> isKilled bus` will result with `true`.
kill :: ∀ a r. Exn.Error -> BusW' r a -> Aff Unit
kill err bus@(Bus cell _) = do
  unlessM (isKilled bus) do
    -- If there are multiple parallel processes executing `kill` at the same time,
    -- then without this try all of processes which are blocked bu put will be killed
    -- as part of handling first `put`. so we have this try to guaranty that kill is idempotent.
    void $ try $ AVar.put (Left err) cell
    -- Here we block until read from `cell` result's with the `error`,
    -- i.e. kill process was finished successfully.
    void $ try $ forever $ AVar.read cell

-- | Synchronously checks whether a Bus has been killed.
isKilled :: ∀ m a r. MonadEffect m ⇒ Bus r a -> m Boolean
isKilled (Bus cell _) = liftEffect $ EffAVar.isKilled <$> EffAVar.status cell
